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

package org.radarbase.passive.phone.telephony

import android.content.ContentResolver
import android.content.Context
import android.content.SharedPreferences
import android.database.Cursor
import android.net.Uri
import android.provider.BaseColumns._ID
import android.provider.CallLog
import android.provider.Telephony
import org.radarbase.android.data.DataCache
import org.radarbase.android.source.AbstractSourceManager
import org.radarbase.android.source.BaseSourceState
import org.radarbase.android.source.SourceStatusListener
import org.radarbase.android.util.HashGenerator
import org.radarbase.android.util.OfflineProcessor
import org.radarcns.kafka.ObservationKey
import org.radarcns.passive.phone.*
import org.slf4j.LoggerFactory
import java.nio.ByteBuffer
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.regex.Pattern

class PhoneLogManager(context: PhoneLogService) : AbstractSourceManager<PhoneLogService, BaseSourceState>(context) {
    private val callTopic: DataCache<ObservationKey, PhoneCall> = createCache("android_phone_call", PhoneCall())
    private val smsTopic: DataCache<ObservationKey, PhoneSms> = createCache("android_phone_sms", PhoneSms())
    private val smsUnreadTopic: DataCache<ObservationKey, PhoneSmsUnread> = createCache("android_phone_sms_unread", PhoneSmsUnread())
    private val preferences: SharedPreferences = context.getSharedPreferences(PhoneLogService::class.java.name, Context.MODE_PRIVATE)
    private val hashGenerator = HashGenerator(context, PhoneLogService::class.java.name)
    private val db: ContentResolver = context.contentResolver
    private val logProcessor: OfflineProcessor
    private var lastSmsTimestamp: Long = 0
    private var lastCallTimestamp: Long = 0

    init {
        name = service.getString(R.string.phoneLogServiceDisplayName)
        logProcessor = OfflineProcessor(context) {
            process = listOf(
                this@PhoneLogManager::processCallLog,
                this@PhoneLogManager::processSmsLog,
                this@PhoneLogManager::processNumberUnreadSms,
            )
            requestCode = REQUEST_CODE_PENDING_INTENT
            requestName = ACTIVITY_LAUNCH_WAKE
            wake = false
        }
    }

    override fun start(acceptableIds: Set<String>) {
        status = SourceStatusListener.Status.READY
        register()
        // Calls and sms, in and outgoing and number of unread sms
        logProcessor.start {
            val now = System.currentTimeMillis()
            lastCallTimestamp = preferences.getLong(LAST_CALL_KEY, now)
            lastSmsTimestamp = preferences.getLong(LAST_SMS_KEY, now)

            if (lastCallTimestamp == now || lastSmsTimestamp == now) {
                logger.info("Setting last SMS / call timestamp to {}", Date(now))
                preferences.edit()
                    .putLong(LAST_CALL_KEY, lastCallTimestamp)
                    .putLong(LAST_SMS_KEY, lastSmsTimestamp)
                    .apply()
            }
        }

        status = SourceStatusListener.Status.CONNECTED
    }

    fun setCallAndSmsLogUpdateRate(period: Long, unit: TimeUnit) {
        // Create pending intent, which cancels currently active pending intent
        logProcessor.interval(period, unit)
        logger.info("Call and SMS log: listener activated and set to a period of {} {}", period, unit)
    }

    private fun processSmsLog() {
        val newSmsTimestamp = processDb(Telephony.Sms.CONTENT_URI, SMS_COLUMNS, Telephony.Sms.DATE, lastSmsTimestamp) {
            val date = getLong(getColumnIndexOrThrow(Telephony.Sms.DATE))

            // If from contact, then the ID of the sender is a non-zero integer
            val isAContact = getInt(getColumnIndexOrThrow(Telephony.Sms.PERSON)) > 0
            sendPhoneSms(date / 1000.0,
                    getString(getColumnIndexOrThrow(Telephony.Sms.ADDRESS)),
                    getInt(getColumnIndexOrThrow(Telephony.Sms.TYPE)),
                    getString(getColumnIndexOrThrow(Telephony.Sms.BODY)),
                    isAContact)

            date
        }

        if (newSmsTimestamp != lastSmsTimestamp) {
            lastSmsTimestamp = newSmsTimestamp
            logger.info("Setting last SMS timestamp to {}", Date(lastSmsTimestamp))

            preferences.edit()
                .putLong(LAST_SMS_KEY, lastSmsTimestamp)
                .apply()
        }
    }

    private fun processCallLog() {
        val newLastCallTimestamp = processDb(CallLog.Calls.CONTENT_URI, CALL_COLUMNS, CallLog.Calls.DATE, lastCallTimestamp) {
            val date = getLong(getColumnIndexOrThrow(CallLog.Calls.DATE))

            // If contact, then the contact lookup uri is given
            val targetIsAContact = getString(getColumnIndexOrThrow(CallLog.Calls.CACHED_LOOKUP_URI)) != null

            sendPhoneCall(date / 1000.0,
                    getString(getColumnIndexOrThrow(CallLog.Calls.NUMBER)),
                    getFloat(getColumnIndexOrThrow(CallLog.Calls.DURATION)),
                    getInt(getColumnIndexOrThrow(CallLog.Calls.TYPE)),
                    targetIsAContact
            )

            date
        }

        if (newLastCallTimestamp != lastCallTimestamp) {
            lastCallTimestamp = newLastCallTimestamp
            logger.info("Setting last call timestamp to {}", Date(lastCallTimestamp))
            preferences.edit()
                .putLong(LAST_CALL_KEY, lastCallTimestamp)
                .apply()
        }
    }

    private fun processDb(contentUri: Uri, columns: Array<String>, dateColumn: String, previousTimestamp: Long, processor: Cursor.() -> Long): Long {
        val where = "$dateColumn > ?"
        val orderBy = "$dateColumn ASC LIMIT $SQLITE_LIMIT"

        var numUpdates: Int
        var lastTimestamp = previousTimestamp

        do {
            val whereArgs = arrayOf(lastTimestamp.toString())
            numUpdates = 0
            // Query all sms with a date later than the last date seen and orderBy by date
            try {
                db.query(contentUri, columns, where, whereArgs, orderBy)?.use { c ->
                    while (c.moveToNext() && !logProcessor.isDone) {
                        numUpdates++
                        lastTimestamp = c.processor()
                    }
                } ?: return lastTimestamp
            } catch (ex: Exception) {
                logger.error("Error in processing the sms log", ex)
            }
        } while (numUpdates == SQLITE_LIMIT && !logProcessor.isDone)

        return lastTimestamp
    }

    private fun processNumberUnreadSms() {
        val where = Telephony.Sms.READ + " = 0"
        try {
            db.query(Telephony.Sms.CONTENT_URI, ID_COLUMNS, where, null, null)?.use { c ->
                sendNumberUnreadSms(c.count)
            }
        } catch (ex: Exception) {
            logger.error("Error in processing the sms log", ex)
        }
    }

    private fun sendPhoneCall(eventTimestamp: Double, target: String, duration: Float, typeCode: Int, targetIsContact: Boolean) {
        val phoneNumber = getNumericPhoneNumber(target)
        val targetKey = createTargetHashKey(target, phoneNumber)

        val type = when(typeCode) {
            CallLog.Calls.INCOMING_TYPE -> PhoneCallType.INCOMING
            CallLog.Calls.OUTGOING_TYPE -> PhoneCallType.OUTGOING
            CallLog.Calls.VOICEMAIL_TYPE -> PhoneCallType.VOICEMAIL
            CallLog.Calls.MISSED_TYPE -> PhoneCallType.MISSED
            else -> PhoneCallType.UNKNOWN
        }

        send(callTopic, PhoneCall(
                eventTimestamp,
                currentTime,
                duration,
                targetKey,
                type,
                targetIsContact,
                phoneNumber == null,
                target.length))
    }

    private fun sendPhoneSms(eventTimestamp: Double, target: String, typeCode: Int, message: String, targetIsContact: Boolean) {
        val phoneNumber = getNumericPhoneNumber(target)
        val targetKey = createTargetHashKey(target, phoneNumber)

        val type = when(typeCode) {
            Telephony.Sms.MESSAGE_TYPE_ALL    -> PhoneSmsType.OTHER
            Telephony.Sms.MESSAGE_TYPE_INBOX  -> PhoneSmsType.INCOMING
            Telephony.Sms.MESSAGE_TYPE_SENT   -> PhoneSmsType.OUTGOING
            Telephony.Sms.MESSAGE_TYPE_DRAFT  -> PhoneSmsType.OTHER
            Telephony.Sms.MESSAGE_TYPE_OUTBOX -> PhoneSmsType.OUTGOING
            Telephony.Sms.MESSAGE_TYPE_FAILED -> PhoneSmsType.OTHER
            Telephony.Sms.MESSAGE_TYPE_QUEUED -> PhoneSmsType.OTHER
            else                              -> PhoneSmsType.UNKNOWN
        }
        val length = message.length

        // Only incoming messages are associated with a contact. For outgoing we don't know
        val sendFromContact: Boolean? = if (type == PhoneSmsType.INCOMING) targetIsContact else null

        send(smsTopic, PhoneSms(
                eventTimestamp,
                currentTime,
                targetKey,
                type,
                length,
                sendFromContact,
                phoneNumber == null,
                target.length))
    }

    private fun sendNumberUnreadSms(numberUnread: Int) {
        val time = currentTime
        send(smsUnreadTopic, PhoneSmsUnread(time, time, numberUnread))
    }

    /**
     * Returns true if target is numeric (e.g. not 'Dropbox' or 'Google')
     * @param target sms/phone target
     * @return boolean
     */
    private fun getNumericPhoneNumber(target: String): Long? {
        return if (IS_NUMBER.matcher(target).matches()) {
            target.toLong()
        } else null
    }

    /**
     * Extracts last 9 characters and hashes the result with a salt.
     * For phone numbers this means that the area code is removed
     * E.g.: +31232014111 becomes 232014111 and 0612345678 becomes 612345678 (before hashing)
     * If target is a name instead of a number (e.g. when sms), then hash this name
     * @param target String
     * @param phoneNumber phone number, null if it is not a number
     * @return MAC-SHA256 encoding of target or null if the target is anonymous
     */
    private fun createTargetHashKey(target: String, phoneNumber: Long?): ByteBuffer? {
        // If non-numerical, then hash the target directly
        return when {
            phoneNumber == null -> hashGenerator.createHashByteBuffer(target)
            phoneNumber < 0 -> null
            else -> {
                // remove international prefixes if present, since that would
                // give inconsistent results -> 0612345678 vs +31612345678
                val phoneNumberSuffix = (phoneNumber % 1_000_000_000L).toInt()
                hashGenerator.createHashByteBuffer(phoneNumberSuffix)
            }
        }
    }

    override fun onClose() {
        logProcessor.close()
    }

    companion object {
        private val logger = LoggerFactory.getLogger(PhoneLogManager::class.java)

        private const val SQLITE_LIMIT = 1000

        private val ID_COLUMNS = arrayOf(_ID)
        private val SMS_COLUMNS = arrayOf(Telephony.Sms.DATE, Telephony.Sms.PERSON, Telephony.Sms.ADDRESS, Telephony.Sms.TYPE, Telephony.Sms.BODY)
        private val CALL_COLUMNS = arrayOf(CallLog.Calls.DATE, CallLog.Calls.CACHED_LOOKUP_URI, CallLog.Calls.NUMBER, CallLog.Calls.DURATION, CallLog.Calls.TYPE)

        // If from contact, then the ID of the sender is a non-zero integer
        private const val LAST_SMS_KEY = "last.sms.time"
        private const val LAST_CALL_KEY = "last.call.time"
        private const val ACTIVITY_LAUNCH_WAKE = "org.radarbase.passive.phone.telephony.PhoneLogManager.ACTIVITY_LAUNCH_WAKE"
        private const val REQUEST_CODE_PENDING_INTENT = 465363071
        private val IS_NUMBER = Pattern.compile("^[+-]?\\d+$")
    }
}
