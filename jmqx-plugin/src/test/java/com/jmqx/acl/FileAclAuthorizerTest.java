package com.jmqx.acl;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FileAclAuthorizerTest {
    @TempDir
    Path tempDir;

    @Test
    void shouldAllowAndDenyByMatchingRules() throws IOException {
        Path ruleFile = tempDir.resolve("acl-rules.txt");
        Files.writeString(
            ruleFile,
            """
            allow subscribe alice sensor/+/status
            deny publish alice sensor/secret
            """
        );

        AclProperties properties = new AclProperties();
        properties.setFilePath(ruleFile.toString());
        FileAclAuthorizer authorizer = new FileAclAuthorizer(properties);

        assertEquals(
            AclDecision.ALLOW,
            authorizer.authorize(new AclRequest("client-1", "alice", "sensor/device-1/status", AclAction.SUBSCRIBE))
        );
        assertEquals(
            AclDecision.DENY,
            authorizer.authorize(new AclRequest("client-1", "alice", "sensor/secret", AclAction.PUBLISH))
        );
        assertEquals(
            AclDecision.NOT_FOUND,
            authorizer.authorize(new AclRequest("client-1", "bob", "sensor/device-1/status", AclAction.SUBSCRIBE))
        );
    }

    @Test
    void shouldTreatWildcardAsteriskAsMultiLevelMatch() throws IOException {
        Path ruleFile = tempDir.resolve("acl-rules.txt");
        Files.writeString(ruleFile, "allow subscribe * *");

        AclProperties properties = new AclProperties();
        properties.setFilePath(ruleFile.toString());
        FileAclAuthorizer authorizer = new FileAclAuthorizer(properties);

        assertEquals(
            AclDecision.ALLOW,
            authorizer.authorize(new AclRequest("client-1", "alice", "sensor/device-1/status", AclAction.SUBSCRIBE))
        );
    }
}
