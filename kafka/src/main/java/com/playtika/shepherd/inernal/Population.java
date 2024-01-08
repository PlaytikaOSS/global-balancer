package com.playtika.shepherd.inernal;

import java.nio.ByteBuffer;
import java.util.Set;

public class Population {

    private final Set<ByteBuffer> sheep;
    private final int version;

    public Population(Set<ByteBuffer> sheep, int version) {
        this.sheep = sheep;
        this.version = version;
    }

    public Set<ByteBuffer> getSheep() {
        return sheep;
    }

    public int getVersion() {
        return version;
    }
}
