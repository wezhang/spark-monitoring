package com.microsoft.pnp;

import mssparkutils.credentialsJ;

public class LogAnalyticsEnvironment {
    public static final String LOG_ANALYTICS_WORKSPACE_ID = "LOG_ANALYTICS_WORKSPACE_ID";
    public static final String LOG_ANALYTICS_WORKSPACE_KEY = "LOG_ANALYTICS_WORKSPACE_KEY";

    public static String getWorkspaceId() {
//        return System.getenv(LOG_ANALYTICS_WORKSPACE_ID);
        return credentialsJ.getSecret("zhwe4spkmonpoc", "workspaceId", "AzureKeyVault1");
    }

    public static String getWorkspaceKey() {
        //return System.getenv(LOG_ANALYTICS_WORKSPACE_KEY);
        return credentialsJ.getSecret("zhwe4spkmonpoc", "workspaceKey", "AzureKeyVault1");
    }
}
