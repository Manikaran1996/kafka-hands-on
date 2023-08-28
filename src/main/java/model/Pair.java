package model;

import lombok.Getter;
import lombok.Setter;

public class Pair<F, S> {

    @Getter
    @Setter
    private F first;

    @Getter
    @Setter
    private S second;

    public Pair(F first, S second) {
        this.first = first;
        this.second = second;
    }


}
