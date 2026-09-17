package dev.netvalve

import android.content.Intent
import android.net.Uri
import android.os.Build
import android.os.Bundle
import android.provider.Settings
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.activity.enableEdgeToEdge
import androidx.activity.result.contract.ActivityResultContracts
import androidx.compose.runtime.remember
import dev.netvalve.service.BatteryOptimizations
import dev.netvalve.service.VpnController
import dev.netvalve.ui.navigation.AppActions
import dev.netvalve.ui.navigation.NetValveNavGraph
import dev.netvalve.ui.theme.NetValveTheme
import dagger.hilt.android.AndroidEntryPoint
import javax.inject.Inject

/**
 * Single-activity Compose host.
 *
 * Owns:
 * - VPN consent
 * - notification permission
 * - Usage Access settings
 * - battery optimization exemption
 * - vendor settings
 * - log export
 */
@AndroidEntryPoint
class MainActivity : ComponentActivity() {

    @Inject
    lateinit var controller: VpnController

    private val consentLauncher =
        registerForActivityResult(
            ActivityResultContracts.StartActivityForResult()
        ) { result ->
            if (result.resultCode == RESULT_OK) {
                controller.start()
            }
        }

    private val notificationsPermission =
        registerForActivityResult(
            ActivityResultContracts.RequestPermission()
        ) {
            // Best effort.
        }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)

        enableEdgeToEdge()

        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
            notificationsPermission.launch(
                android.Manifest.permission.POST_NOTIFICATIONS
            )
        }

        setContent {
            NetValveTheme {
                val actions = remember {
                    buildActions()
                }

                NetValveNavGraph(
                    actions = actions
                )
            }
        }
    }

    private fun buildActions() = AppActions(

        // ---------------------------------------------------------
        // VPN
        // ---------------------------------------------------------
        toggleVpn = { enable ->
            if (enable) {
                val consent = controller.consentIntent()

                if (consent != null) {
                    consentLauncher.launch(consent)
                } else {
                    controller.start()
                }
            } else {
                controller.stop()
            }
        },

        restartTunnel = {
            controller.restart()
        },

        // ---------------------------------------------------------
        // USAGE ACCESS
        //
        // Open the Usage Access settings directly for this package.
        // This avoids relying on HiOS displaying NetValve correctly
        // in the global Usage Access application list.
        // ---------------------------------------------------------
        requestUsageAccess = {
            runCatching {

                val intent = Intent(
                    Settings.ACTION_USAGE_ACCESS_SETTINGS
                ).apply {
                    data = Uri.parse("package:$packageName")
                    addFlags(Intent.FLAG_ACTIVITY_NEW_TASK)
                }

                startActivity(intent)
            }
        },

        // ---------------------------------------------------------
        // BATTERY OPTIMIZATION
        // ---------------------------------------------------------
        requestBatteryExemption = {
            runCatching {
                startActivity(
                    BatteryOptimizations.requestExemptionIntent(this)
                )
            }
        },

        // ---------------------------------------------------------
        // OEM / VENDOR SETTINGS
        // ---------------------------------------------------------
        openVendorSettings = {
            BatteryOptimizations
                .vendorHint(this)
                ?.settingsIntent
                ?.let { intent ->
                    runCatching {
                        startActivity(intent)
                    }
                }
        },

        // ---------------------------------------------------------
        // LOG EXPORT
        // ---------------------------------------------------------
        exportLogs = { text ->
            shareText(text)
        }
    )

    private fun shareText(text: String) {

        val send = Intent(Intent.ACTION_SEND).apply {
            type = "text/plain"

            putExtra(
                Intent.EXTRA_SUBJECT,
                "NetValve logs"
            )

            putExtra(
                Intent.EXTRA_TEXT,
                text
            )
        }

        runCatching {
            startActivity(
                Intent.createChooser(
                    send,
                    "Export logs"
                )
            )
        }
    }
}