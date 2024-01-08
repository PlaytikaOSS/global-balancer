package com.playtika.shepherd.inernal;

import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.apache.kafka.common.protocol.types.Type.BYTES;

public class ProtocolHelper {

    public static final String LEADER_KEY_NAME = "leader";
    public static final String VERSION_KEY_NAME = "version";
    public static final String ASSIGNED_KEY_NAME = "assigned";

    public static final Schema ASSIGNMENT_V0 = new Schema(
            new Field(LEADER_KEY_NAME, Type.STRING),
            new Field(VERSION_KEY_NAME, Type.INT32),
            new Field(ASSIGNED_KEY_NAME, new ArrayOf(BYTES)));

    public static Assignment deserializeAssignment(ByteBuffer buffer) {
        Struct struct = ASSIGNMENT_V0.read(buffer);
        String leader = struct.getString(LEADER_KEY_NAME);
        int version = struct.getInt(VERSION_KEY_NAME);
        List<ByteBuffer> assigned = new ArrayList<>();
        for (Object element : struct.getArray(ASSIGNED_KEY_NAME)) {
            assigned.add((ByteBuffer) element);
        }
        return new Assignment(leader, version, assigned);
    }

    public static ByteBuffer serializeAssignment(Assignment assignment) {

        Struct struct = new Struct(ASSIGNMENT_V0);
        struct.set(LEADER_KEY_NAME, assignment.getLeader());
        struct.set(VERSION_KEY_NAME, assignment.getVersion());
        struct.set(ASSIGNED_KEY_NAME, assignment.getAssigned().toArray());

        ByteBuffer buffer = ByteBuffer.allocate(ASSIGNMENT_V0.sizeOf(struct));
        ASSIGNMENT_V0.write(buffer, struct);
        buffer.flip();
        return buffer;
    }

}
