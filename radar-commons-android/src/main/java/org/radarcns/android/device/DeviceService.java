/*
 * Copyright 2017 The Hyve
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.radarcns.android.device;

import android.app.Service;
import android.bluetooth.BluetoothAdapter;
import android.content.BroadcastReceiver;
import android.content.Context;
import android.content.Intent;
import android.content.IntentFilter;
import android.os.Binder;
import android.os.Bundle;
import android.os.Handler;
import android.os.IBinder;
import android.os.Parcel;
import android.os.RemoteException;
import android.os.ResultReceiver;
import android.support.annotation.CallSuper;
import android.support.annotation.NonNull;
import android.support.annotation.Nullable;
import android.support.v4.content.LocalBroadcastManager;
import android.util.Pair;

import org.apache.avro.specific.SpecificRecord;
import org.radarcns.android.RadarApplication;
import org.radarcns.android.RadarConfiguration;
import org.radarcns.android.auth.AppAuthState;
import org.radarcns.android.auth.AppSource;
import org.radarcns.android.auth.portal.ManagementPortalService;
import org.radarcns.android.data.DataCache;
import org.radarcns.android.data.TableDataHandler;
import org.radarcns.android.kafka.ServerStatusListener;
import org.radarcns.android.util.BundleSerialization;
import org.radarcns.data.Record;
import org.radarcns.kafka.ObservationKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import static org.radarcns.android.RadarConfiguration.SOURCE_ID_KEY;
import static org.radarcns.android.auth.portal.ManagementPortalService.MANAGEMENT_PORTAL_REGISTRATION;
import static org.radarcns.android.device.DeviceServiceProvider.NEEDS_BLUETOOTH_KEY;
import static org.radarcns.android.device.DeviceServiceProvider.SOURCE_KEY;

/**
 * A service that manages a DeviceManager and a TableDataHandler to send addToPreferences the data of a
 * wearable device and send it to a Kafka REST proxy.
 *
 * Specific wearables should extend this class.
 */
@SuppressWarnings("WeakerAccess")
public abstract class DeviceService<T extends BaseDeviceState> extends Service implements DeviceStatusListener {
    private static final String PREFIX = "org.radarcns.android.";
    public static final String SERVER_STATUS_CHANGED = PREFIX + "ServerStatusListener.Status";
    public static final String SERVER_RECORDS_SENT_TOPIC = PREFIX + "ServerStatusListener.topic";
    public static final String SERVER_RECORDS_SENT_NUMBER = PREFIX + "ServerStatusListener.lastNumberOfRecordsSent";
    public static final String CACHE_TOPIC = PREFIX + "DataCache.topic";
    public static final String CACHE_RECORDS_UNSENT_NUMBER = PREFIX + "DataCache.numberOfRecords.first";
    public static final String CACHE_RECORDS_SENT_NUMBER = PREFIX + "DataCache.numberOfRecords.second";
    public static final String DEVICE_SERVICE_CLASS = PREFIX + "DeviceService.getClass";
    public static final String DEVICE_STATUS_CHANGED = PREFIX + "DeviceStatusListener.Status";
    public static final String DEVICE_STATUS_NAME = PREFIX + "DeviceManager.getName";
    public static final String DEVICE_CONNECT_FAILED = PREFIX + "DeviceStatusListener.deviceFailedToConnect";

    private final ObservationKey key = new ObservationKey();

    /** Stops the device when bluetooth is disabled. */
    private final BroadcastReceiver mBluetoothReceiver = new BroadcastReceiver() {
        @Override
        public void onReceive(Context context, Intent intent) {
            final String action = intent.getAction();

            if (Objects.equals(action, BluetoothAdapter.ACTION_STATE_CHANGED)) {
                final int state = intent.getIntExtra(
                        BluetoothAdapter.EXTRA_STATE, BluetoothAdapter.ERROR);
                switch (state) {
                    case BluetoothAdapter.STATE_TURNING_OFF: case BluetoothAdapter.STATE_OFF:
                        logger.warn("Bluetooth is off");
                        stopDeviceManager(unsetDeviceManager());
                        break;
                    default:
                        logger.debug("Bluetooth is in state {}", state);
                        break;
                }
            }
        }
    };

    private static final Logger logger = LoggerFactory.getLogger(DeviceService.class);
    private TableDataHandler dataHandler;
    private DeviceManager<T> deviceScanner;
    private DeviceBinder mBinder;
    private boolean hasBluetoothPermission;
    private AppSource source;

    @CallSuper
    @Override
    public void onCreate() {
        logger.info("Creating DeviceService {}", this);
        super.onCreate();
        mBinder = createBinder();

        if (isBluetoothConnectionRequired()) {
            IntentFilter intentFilter = new IntentFilter(BluetoothAdapter.ACTION_STATE_CHANGED);
            registerReceiver(mBluetoothReceiver, intentFilter);
        }

        synchronized (this) {
            deviceScanner = null;
        }
    }

    @CallSuper
    @Override
    public void onDestroy() {
        logger.info("Destroying DeviceService {}", this);
        super.onDestroy();

        if (isBluetoothConnectionRequired()) {
            // Unregister broadcast listeners
            unregisterReceiver(mBluetoothReceiver);
        }
        stopDeviceManager(unsetDeviceManager());
        ((RadarApplication)getApplicationContext()).onDeviceServiceDestroy(this);

        if (dataHandler != null) {
            try {
                dataHandler.close();
            } catch (IOException e) {
                // do nothing
            }
        }
    }

    @Nullable
    @Override
    public IBinder onBind(Intent intent) {
        doBind(intent, true);
        return mBinder;
    }

    @CallSuper
    @Override
    public void onRebind(Intent intent) {
        doBind(intent, false);
    }

    private void doBind(Intent intent, boolean firstBind) {
        logger.info("Received (re)bind in {}", this);
        Bundle extras = BundleSerialization.getPersistentExtras(intent, this);
        onInvocation(extras);

        RadarApplication application = (RadarApplication)getApplicationContext();
        application.onDeviceServiceInvocation(this, extras, firstBind);
    }

    @Override
    public boolean onUnbind(Intent intent) {
        logger.info("Received unbind in {}", this);
        return true;
    }

    @Override
    public void deviceFailedToConnect(String deviceName) {
        Intent statusChanged = new Intent(DEVICE_CONNECT_FAILED);
        statusChanged.putExtra(DEVICE_SERVICE_CLASS, getClass().getName());
        statusChanged.putExtra(DEVICE_STATUS_NAME, deviceName);
        LocalBroadcastManager.getInstance(this).sendBroadcast(statusChanged);
    }


    private void broadcastDeviceStatus(String name, DeviceStatusListener.Status status) {
        Intent statusChanged = new Intent(DEVICE_STATUS_CHANGED);
        statusChanged.putExtra(DEVICE_STATUS_CHANGED, status.ordinal());
        statusChanged.putExtra(DEVICE_SERVICE_CLASS, getClass().getName());
        if (name != null) {
            statusChanged.putExtra(DEVICE_STATUS_NAME, name);
        }
        LocalBroadcastManager.getInstance(this).sendBroadcast(statusChanged);
    }

    @Override
    public void deviceStatusUpdated(DeviceManager deviceManager, DeviceStatusListener.Status status) {
        switch (status) {
            case DISCONNECTED:
                stopDeviceManager(deviceManager);
                synchronized (this) {
                    deviceScanner = null;
                }
                break;
            default:
                // do nothing
                break;
        }
        broadcastDeviceStatus(deviceManager.getName(), status);
    }

    private synchronized DeviceManager unsetDeviceManager() {
        DeviceManager tmpManager = deviceScanner;
        deviceScanner = null;
        return tmpManager;
    }

    private void stopDeviceManager(DeviceManager deviceManager) {
        if (deviceManager != null) {
            if (!deviceManager.isClosed()) {
                try {
                    deviceManager.close();
                } catch (IOException e) {
                    logger.warn("Failed to close device scanner", e);
                }
            }
        }
    }

    /**
     * New device manager for the current device.
     */
    protected abstract DeviceManager<T> createDeviceManager();

    protected T getState() {
        DeviceManager<T> localManager = getDeviceManager();
        if (localManager != null) {
            return localManager.getState();
        }

        T state = getDefaultState();
        ObservationKey stateKey = state.getId();
        stateKey.setProjectId(key.getProjectId());
        stateKey.setUserId(key.getUserId());
        stateKey.setSourceId(key.getSourceId());
        return state;
    }

    /**
     * Default state when no device manager is active.
     */
    protected abstract T getDefaultState();

    public T startRecording(@NonNull Set<String> acceptableIds) {
        DeviceManager localManager = getDeviceManager();
        if (key.getUserId() == null) {
            throw new IllegalStateException("Cannot start recording: user ID is not set.");
        }
        if (localManager == null) {
            if (isBluetoothConnectionRequired() && BluetoothAdapter.getDefaultAdapter() != null && !BluetoothAdapter.getDefaultAdapter().isEnabled()) {
                throw new IllegalStateException("Cannot start recording without Bluetooth");
            }
            logger.info("Starting recording");
            synchronized (this) {
                if (deviceScanner == null) {
                    deviceScanner = createDeviceManager();
                    deviceScanner.start(acceptableIds);
                }
            }
        }
        return getDeviceManager().getState();
    }

    public void stopRecording() {
        stopDeviceManager(unsetDeviceManager());
        logger.info("Stopped recording {}", this);
    }

    protected class DeviceBinder extends Binder implements DeviceServiceBinder {
        @Override
        public <V extends SpecificRecord> List<Record<ObservationKey, V>> getRecords(
                @NonNull String topic, int limit) throws IOException {
            TableDataHandler localDataHandler = getDataHandler();
            if (localDataHandler == null) {
                return Collections.emptyList();
            }
            return localDataHandler.<V>getCache(topic).getRecords(limit);
        }

        @Override
        public T getDeviceStatus() {
            return getState();
        }

        @Override
        public String getDeviceName() {
            DeviceManager localManager = getDeviceManager();
            if (localManager == null) {
                return null;
            }
            return localManager.getName();
        }

        @Override
        public T startRecording(@NonNull Set<String> acceptableIds) {
            return DeviceService.this.startRecording(acceptableIds);
        }

        @Override
        public void stopRecording() {
            DeviceService.this.stopRecording();
        }

        @Override
        public ServerStatusListener.Status getServerStatus() {
            TableDataHandler localDataHandler = getDataHandler();
            if (localDataHandler == null) {
                return ServerStatusListener.Status.DISCONNECTED;
            }
            return localDataHandler.getStatus();
        }

        @Override
        public Map<String,Integer> getServerRecordsSent() {
            TableDataHandler localDataHandler = getDataHandler();
            if (localDataHandler == null) {
                return Collections.emptyMap();
            }
            return localDataHandler.getRecordsSent();
        }

        @Override
        public void updateConfiguration(Bundle bundle) {
            onInvocation(bundle);
        }

        @Override
        public Pair<Long, Long> numberOfRecords() {
            long unsent = -1L;
            long sent = -1L;
            TableDataHandler localDataHandler = getDataHandler();
            if (localDataHandler != null) {
                for (DataCache<?, ?> cache : localDataHandler.getCaches().values()) {
                    Pair<Long, Long> pair = cache.numberOfRecords();
                    if (pair.first != -1L) {
                        if (unsent == -1L) {
                            unsent = pair.first;
                        } else {
                            unsent += pair.first;
                        }
                    }
                    if (pair.second != -1L) {
                        if (sent == -1L) {
                            sent = pair.second;
                        } else {
                            sent += pair.second;
                        }
                    }
                }
            }
            return new Pair<>(unsent, sent);
        }

        @Override
        public boolean needsBluetooth() {
            return isBluetoothConnectionRequired();
        }

        @Override
        public void setDataHandler(TableDataHandler dataHandler) {
            DeviceService.this.setDataHandler(dataHandler);
        }

        @Override
        public boolean onTransact(int code, Parcel data, Parcel reply, int flags) throws RemoteException {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Override this function to get any parameters from the given intent.
     * Bundle classloader needs to be set correctly for this to work.
     *
     * @param bundle intent extras that the activity provided.
     */
    @CallSuper
    protected void onInvocation(@NonNull Bundle bundle) {
        source = bundle.getParcelable(SOURCE_KEY);
        if (source == null) {
            source = new AppSource(-1L, null, null, null, true);
        }
        if (source.getSourceId() != null) {
            key.setSourceId(source.getSourceId());
        }
        AppAuthState authState = AppAuthState.Builder.from(bundle).build();
        key.setProjectId(authState.getProjectId());
        key.setUserId(authState.getUserId());

        setHasBluetoothPermission(bundle.getBoolean(NEEDS_BLUETOOTH_KEY, false));
    }

    public synchronized TableDataHandler getDataHandler() {
        return dataHandler;
    }

    public synchronized void setDataHandler(TableDataHandler dataHandler) {
        this.dataHandler = dataHandler;
    }

    public synchronized DeviceManager<T> getDeviceManager() {
        return deviceScanner;
    }

    /** Get the service local binder. */
    @NonNull
    protected DeviceBinder createBinder() {
        return new DeviceBinder();
    }

    protected void setHasBluetoothPermission(boolean isRequired) {
        boolean oldBluetoothNeeded = isBluetoothConnectionRequired();
        hasBluetoothPermission = isRequired;
        boolean newBluetoothNeeded = isBluetoothConnectionRequired();

        if (oldBluetoothNeeded && !newBluetoothNeeded) {
            unregisterReceiver(mBluetoothReceiver);
        } else if (!oldBluetoothNeeded && newBluetoothNeeded) {
            // Register for broadcasts on BluetoothAdapter state change
            IntentFilter filter = new IntentFilter(BluetoothAdapter.ACTION_STATE_CHANGED);
            registerReceiver(mBluetoothReceiver, filter);
        }
    }

    protected boolean hasBluetoothPermission() {
        return hasBluetoothPermission;
    }

    protected boolean isBluetoothConnectionRequired() {
        return hasBluetoothPermission();
    }

    public ObservationKey getKey() {
        return key;
    }

    public AppSource getSource() {
        return source;
    }

    public void registerDevice(String name, Map<String, String> attributes) {
        registerDevice(null, name, attributes);
    }

    public void registerDevice(String sourceIdHint, String name, Map<String, String> attributes) {
        logger.info("Registering source {} with attributes {}", source, attributes);
        if (source.getSourceId() != null) {
            DeviceManager<T> localManager = getDeviceManager();
            if (localManager != null) {
                localManager.didRegister(source);
            }
            return;
        }
        source.setSourceName(name);
        source.setAttributes(attributes);
        // not yet registered
        if (source.getSourceId() == null) {
            if (ManagementPortalService.isEnabled()) {
                // do registration with management portal
                final Handler handler = new Handler(getMainLooper());
                ManagementPortalService.registerSource(this, source,
                        new ResultReceiver(handler) {
                    @Override
                    protected void onReceiveResult(int resultCode, Bundle result) {
                        updateRegistration(resultCode, result, handler);
                    }
                });
                return;
            } else {
                // self-register
                if (sourceIdHint == null) {
                    source.setSourceId(RadarConfiguration.getOrSetUUID(this, SOURCE_ID_KEY));
                } else {
                    source.setSourceId(sourceIdHint);
                }
                key.setSourceId(source.getSourceId());
            }
        }
        DeviceManager<T> localManager = getDeviceManager();
        if (localManager != null) {
            localManager.didRegister(source);
        }
    }

    private void updateRegistration(int resultCode, Bundle result, Handler handler) {
        AppSource updatedSource = null;
        if (resultCode == MANAGEMENT_PORTAL_REGISTRATION && result != null && result.containsKey(SOURCE_KEY)) {
            updatedSource = result.getParcelable(SOURCE_KEY);
        }
        if (updatedSource == null) {
            // try again in a minute
            handler.postDelayed(new Runnable() {
                @Override
                public void run() {
                    stopDeviceManager(unsetDeviceManager());
                }
            }, ThreadLocalRandom.current().nextLong(1_000L, 120_000L));
            return;
        }
        AppAuthState auth = AppAuthState.Builder.from(result).build();
        if (auth.isInvalidated()) {
            logger.info("New source ID requires new OAuth2 JWT token.");
            Intent statusIntent = new Intent(SERVER_STATUS_CHANGED);
            statusIntent.putExtra(SERVER_STATUS_CHANGED, ServerStatusListener.Status.UNAUTHORIZED.ordinal());
            statusIntent.putExtra(DEVICE_SERVICE_CLASS, getClass().getName());
            sendBroadcast(statusIntent);
        }
        key.setProjectId(auth.getProjectId());
        key.setUserId(auth.getUserId());
        key.setSourceId(updatedSource.getSourceId());
        source.setSourceId(updatedSource.getSourceId());
        source.setSourceName(updatedSource.getSourceName());
        source.setExpectedSourceName(updatedSource.getExpectedSourceName());
        DeviceManager<T> localManager = getDeviceManager();
        if (localManager != null) {
            localManager.didRegister(source);
        }
    }

    @Override
    public String toString() {
        DeviceManager localManager = getDeviceManager();
        if (localManager == null) {
            return getClass().getSimpleName() + "<null>";
        } else {
            return getClass().getSimpleName() + "<" + localManager.getName() + ">";
        }
    }
}
