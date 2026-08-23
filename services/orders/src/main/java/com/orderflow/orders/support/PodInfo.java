package com.orderflow.orders.support;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Kubernetes sets HOSTNAME to the pod name for every container. Every API
 * response includes this so a curl loop or the checkout console can show
 * which replica actually answered a given request.
 */
public final class PodInfo {

    private static final String HOSTNAME = resolveHostname();

    private PodInfo() {
    }

    public static String hostname() {
        return HOSTNAME;
    }

    private static String resolveHostname() {
        String fromEnv = System.getenv("HOSTNAME");
        if (fromEnv != null && !fromEnv.isBlank()) {
            return fromEnv;
        }
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "unknown";
        }
    }
}
