package org.isaacanteparac;

public enum Topics {

    DURAN_IN("duran-IN"),
    DURAN_OUT("duran-OUT"),
    SAMBORONDON_IN("samborondon-IN"),
    SAMBORONDON_OUT("samborondon-OUT");

    private String name;

    Topics(String name) {
        this.name = name;
    }

    public String getName(){
        return name;
    }
}
