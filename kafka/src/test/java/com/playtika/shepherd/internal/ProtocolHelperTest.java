package com.playtika.shepherd.internal;

import com.playtika.shepherd.inernal.Assignment;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;

import static com.playtika.shepherd.inernal.ProtocolHelper.deserializeAssignment;
import static com.playtika.shepherd.inernal.ProtocolHelper.serializeAssignment;
import static org.assertj.core.api.Assertions.assertThat;

public class ProtocolHelperTest {

    @Test
    public void shouldDeSer(){
        Assignment assignment = new Assignment("node-1", 7, List.of(
                ByteBuffer.wrap(new byte[]{1, 2}),
                ByteBuffer.wrap(new byte[]{3, 4})));

        ByteBuffer serialized = serializeAssignment(assignment);
        Assignment deserialized = deserializeAssignment(serialized);

        assertThat(deserialized).isEqualTo(assignment);
    }

}
