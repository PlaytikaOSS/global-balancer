package com.playtika.shepherd.inernal;

import org.apache.kafka.common.message.JoinGroupResponseData;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface Assignor {
    Map<String, ByteBuffer> performAssignment(
            String leaderId, String protocol, Set<ByteBuffer> population, int version,
            List<JoinGroupResponseData.JoinGroupResponseMember> allMemberMetadata);
}
