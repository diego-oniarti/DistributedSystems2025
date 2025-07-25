package org.example.shared;

import java.util.ArrayList;
import java.util.Random;

public class RngList<T> extends ArrayList<T> {
    private Random rng;

    public RngList(Random rng) {
        super();
        this.rng = rng;
    }
    public RngList() {
        this(new Random());
    }

    public T getRandom() {
        if (this.isEmpty()) return null;
        return this.get(rng.nextInt(this.size()));
    }

    public T removeRandom() {
        if (this.isEmpty()) return null;
        return this.remove(rng.nextInt(this.size()));
    }
}
