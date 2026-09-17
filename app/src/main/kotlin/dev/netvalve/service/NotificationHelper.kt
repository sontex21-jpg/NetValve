package dev.netvalve.service

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.content.Context
import android.content.Intent
import androidx.core.app.NotificationCompat
import dev.netvalve.MainActivity
import dev.netvalve.stats.StatsSnapshot
import dev.netvalve.utils.Format

/**
 * Builds the ongoing foreground-service notification and its channel.
 *
 * This implementation intentionally does not depend on Android resources
 * such as R.string or R.drawable because this project currently has no
 * app/src/main/res directory.
 */
object NotificationHelper {

    const val CHANNEL_ID = "netvalve_tunnel"
    const val NOTIFICATION_ID = 1001

    private const val CHANNEL_NAME = "NetValve VPN"
    private const val CHANNEL_DESCRIPTION = "NetValve traffic controller"

    private const val NOTIFICATION_TITLE = "NetValve"
    private const val ACTION_STOP_LABEL = "Stop"
    private const val ACTION_PAUSE_LABEL = "Pause"
    private const val ACTION_RESUME_LABEL = "Resume"

    fun ensureChannel(context: Context) {
        val manager =
            context.getSystemService(NotificationManager::class.java)

        if (manager.getNotificationChannel(CHANNEL_ID) == null) {
            val channel = NotificationChannel(
                CHANNEL_ID,
                CHANNEL_NAME,
                NotificationManager.IMPORTANCE_LOW
            ).apply {
                description = CHANNEL_DESCRIPTION
                setShowBadge(false)
            }

            manager.createNotificationChannel(channel)
        }
    }

    fun build(
        context: Context,
        status: VpnStatus,
        snapshot: StatsSnapshot
    ): Notification {

        val contentIntent = PendingIntent.getActivity(
            context,
            0,
            Intent(context, MainActivity::class.java),
            PendingIntent.FLAG_IMMUTABLE or
                PendingIntent.FLAG_UPDATE_CURRENT
        )

        val stopIntent = PendingIntent.getService(
            context,
            1,
            Intent(context, NetValveVpnService::class.java)
                .setAction(VpnActions.ACTION_STOP),
            PendingIntent.FLAG_IMMUTABLE or
                PendingIntent.FLAG_UPDATE_CURRENT
        )

        val isPaused = status.state == TunnelState.PAUSED

        val pauseResumeAction =
            if (isPaused) {
                VpnActions.ACTION_RESUME to ACTION_RESUME_LABEL
            } else {
                VpnActions.ACTION_PAUSE to ACTION_PAUSE_LABEL
            }

        val pauseIntent = PendingIntent.getService(
            context,
            2,
            Intent(context, NetValveVpnService::class.java)
                .setAction(pauseResumeAction.first),
            PendingIntent.FLAG_IMMUTABLE or
                PendingIntent.FLAG_UPDATE_CURRENT
        )

        val text =
            "Apps: ${status.controlledAppCount}  " +
            "↓ ${Format.rate(snapshot.liveDownloadBps)}  " +
            "↑ ${Format.rate(snapshot.liveUploadBps)}"

        return NotificationCompat.Builder(
            context,
            CHANNEL_ID
        )
            .setSmallIcon(android.R.drawable.stat_sys_upload)
            .setContentTitle(NOTIFICATION_TITLE)
            .setContentText(text)
            .setContentIntent(contentIntent)
            .setOngoing(true)
            .setOnlyAlertOnce(true)
            .setCategory(Notification.CATEGORY_SERVICE)
            .setPriority(NotificationCompat.PRIORITY_LOW)
            .addAction(
                0,
                ACTION_STOP_LABEL,
                stopIntent
            )
            .addAction(
                0,
                pauseResumeAction.second,
                pauseIntent
            )
            .build()
    }
}