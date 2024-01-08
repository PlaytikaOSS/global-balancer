package com.playtika.shepherd.inernal.utils;

import java.nio.ByteBuffer;

public class BytesUtils {

    public static byte[] getBytes(ByteBuffer buf){
        byte[] arr = new byte[buf.remaining()];
        buf.get(arr);
        buf.rewind();
        return arr;
    }
}
