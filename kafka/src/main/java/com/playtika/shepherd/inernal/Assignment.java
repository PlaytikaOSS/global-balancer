package com.playtika.shepherd.inernal;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

public class Assignment {

    private final String leader;
    private final List<ByteBuffer> assigned;
    private final int version;

    public Assignment(String leader, int version, List<ByteBuffer> assigned) {
        this.leader = leader;
        this.assigned = assigned;
        this.version = version;
    }

    public String getLeader() {
        return leader;
    }

    public List<ByteBuffer> getAssigned() {
        return assigned;
    }

    public int getVersion() {
        return version;
    }

    @Override
    public boolean equals(Object obj){
        Assignment assignment = (Assignment) obj;
        return Objects.equals(leader, assignment.leader)
                && Objects.equals(assigned, assignment.assigned);
    }

}
