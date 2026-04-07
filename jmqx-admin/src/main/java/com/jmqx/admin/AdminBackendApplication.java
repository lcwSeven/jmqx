package com.jmqx.admin;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * @author liucaiwen
 * @date 2026/4/5
 */
@SpringBootApplication
public class AdminBackendApplication {
    public static void main(String[] args) {
        configureFrontend();
        SpringApplication.run(AdminBackendApplication.class, args);
    }

    private static void configureFrontend() {
        AdminProperties defaults = new AdminProperties();
        boolean integrated = getBoolean("jmqx.admin.frontend.integrated", defaults.isFrontendIntegrated());
        if (!integrated) {
            return;
        }

        boolean buildOnStart = getBoolean("jmqx.admin.frontend.buildOnStart", defaults.isFrontendBuildOnStart());
        String workDirRaw = getString("jmqx.admin.frontend.build.workDir", defaults.getFrontendBuildWorkDir());
        String buildCommand = getString("jmqx.admin.frontend.build.command", defaults.getFrontendBuildCommand());
        String distDirRaw = getString("jmqx.admin.frontend.distDir", defaults.getFrontendDistDir());

        Path baseDir = Paths.get("").toAbsolutePath().normalize();
        Path workDir = baseDir.resolve(workDirRaw).normalize();
        Path distDir = baseDir.resolve(distDirRaw).normalize();

        if (buildOnStart && workDir.toFile().isDirectory() && buildCommand != null && !buildCommand.isBlank()) {
            try {
                String os = System.getProperty("os.name", "").toLowerCase();
                ProcessBuilder pb = os.contains("win")
                    ? new ProcessBuilder("cmd", "/c", buildCommand)
                    : new ProcessBuilder("sh", "-c", buildCommand);
                pb.directory(workDir.toFile());
                pb.inheritIO();
                Process process = pb.start();
                process.waitFor();
            } catch (IOException | InterruptedException ignored) {
                if (ignored instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        String staticLocations = "classpath:/static/,file:" + distDir + "/";
        System.setProperty("spring.web.resources.static-locations", staticLocations);
    }

    private static String getString(String key, String defaultValue) {
        String value = System.getProperty(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return value;
    }

    private static boolean getBoolean(String key, boolean defaultValue) {
        String value = System.getProperty(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        if ("true".equalsIgnoreCase(value) || "1".equals(value) || "yes".equalsIgnoreCase(value)) {
            return true;
        }
        if ("false".equalsIgnoreCase(value) || "0".equals(value) || "no".equalsIgnoreCase(value)) {
            return false;
        }
        return defaultValue;
    }
}
