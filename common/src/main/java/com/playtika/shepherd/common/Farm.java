package com.playtika.shepherd.common;

/**
 * Farm has many herds distributed evenly by pastures
 */
public interface Farm {

    /**
     * Here we come to Farm with our Pasture to graze specific Herd on it.
     *
     * @param herdName  Herd we want to inhabit this pasture
     * @param pastureListener  Will listen for animals from Herd that will be assigned to our Pasture
     * @return Shepherd allows to set Herd population
     */
    Pasture addPasture(String herdName, PastureListener pastureListener);

}
