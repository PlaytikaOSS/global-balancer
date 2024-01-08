package com.playtika.shepherd.common;

import java.time.Duration;

public interface Pasture {

    Shepherd getShepherd();

    void close(Duration timeout);
}
