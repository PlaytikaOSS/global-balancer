package com.playtika.shepherd.inernal;

public interface Herd {

    /**
     * Assignor will request it once on assignment
     * @return creatures that should be evenly distributed across pastures
     */
    Population getPopulation();

    /**
     * called each time on first phase of rebalance
     */
    void reset();

}
