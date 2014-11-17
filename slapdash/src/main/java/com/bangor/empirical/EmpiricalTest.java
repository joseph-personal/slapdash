package com.bangor.empirical;

import org.kohsuke.args4j.Option;

/**
 * Created by Joseph W Plant on 17/11/2014.
 */
public abstract class EmpiricalTest {

    @Option(name="-Hadoop", usage="specifies this test is to be run on a Hadoop cluster")
    protected boolean isHadoop;

    protected String sFileName = "part-r-00000";
    /**
     * Executes the Test based on parameters parsed by args4j
     */
    public abstract void executeTest();
}
