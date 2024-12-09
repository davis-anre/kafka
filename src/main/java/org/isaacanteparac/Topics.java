package org.isaacanteparac;

public enum Topics {

    DURAN_PRODUCER("duran-IN"),
    DURAN_CONSUMER("duran-OUT"),
    SAMBORONDON_PRODUCER("samborondon-IN"),
    SAMBORONDON_CONSUMER("samborondon-OUT");

    private String name;

    Topics(String name) {
        this.name = name;
    }

    public String getName(){
        return name;
    }
}
