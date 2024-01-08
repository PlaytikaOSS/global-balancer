package com.playtika.shepherd.inernal;

public class CheckedHerd implements Herd {

    private final Herd herd;
    private boolean requested = false;

    public static Herd checked(Herd herd){
        return new CheckedHerd(herd);
    }

    private CheckedHerd(Herd herd) {
        this.herd = herd;
    }

    public Population getPopulation(){
        if(requested){
            throw new IllegalStateException("Should be called only once on rebalance");
        }
        try {
            return herd.getPopulation();
        } finally {
            requested = true;
        }
    }

    @Override
    public void reset() {
        herd.reset();
        requested = false;
    }
}
