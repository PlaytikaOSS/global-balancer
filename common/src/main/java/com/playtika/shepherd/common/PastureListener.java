package com.playtika.shepherd.common;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Invoked when new population assigned to pasture
 */
public interface PastureListener {

    void assigned(List<ByteBuffer> population, int version, int generation, boolean isLeader);

}
