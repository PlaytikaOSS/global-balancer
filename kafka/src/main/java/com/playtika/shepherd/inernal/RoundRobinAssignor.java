package com.playtika.shepherd.inernal;

import org.apache.kafka.common.message.JoinGroupResponseData;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.playtika.shepherd.inernal.ProtocolHelper.serializeAssignment;

/**
 * Sort population lexicographically before assignment in round-robin way
 */
public class RoundRobinAssignor implements Assignor {

    @Override
    public Map<String, ByteBuffer> performAssignment(
            String leaderId, String protocol, Set<ByteBuffer> population, int version,
            List<JoinGroupResponseData.JoinGroupResponseMember> allMemberMetadata) {

        ArrayList<ByteBuffer> cows = new ArrayList<>(population);
        Collections.sort(cows);

        int herdSize = cows.size();
        int clusterSize = allMemberMetadata.size();
        int cowsPerMember = herdSize / clusterSize + 1;

        List<Assignment> assignments = allMemberMetadata.stream()
                .map(member -> new Assignment(leaderId, version, new ArrayList<>(cowsPerMember)))
                .toList();

        for(int cowId = 0; cowId < herdSize; cowId++){
            assignments.get(cowId % clusterSize).getAssigned().add(cows.get(cowId));
        }

        Map<String, ByteBuffer> assignmentsMap = new HashMap<>(assignments.size());
        for(int assignmentId = 0; assignmentId < clusterSize; assignmentId++){
            assignmentsMap.put(allMemberMetadata.get(assignmentId).memberId(), serializeAssignment(assignments.get(assignmentId)));
        }
        return assignmentsMap;
    }
}
