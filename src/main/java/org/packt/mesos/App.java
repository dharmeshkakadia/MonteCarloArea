package org.packt.mesos;

import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;

import java.io.File;
import java.util.Arrays;


public class App {
    public static void main(String[] args) {
        if (args.length < 8) {
            System.err.println("Usage: MonteCarloScheduler <Master URI>  <Number of Tasks> <Curve Expression> <xLow> <xHigh> <yLow> <yHigh> <Number of Points>");
            System.exit(-1);
        }
        // Get the path of the JAR
        String JAR_PATH = null;
        try {
            JAR_PATH = new File(MesosSchedulerDriver.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath()).getPath();
        } catch (Exception e) {
            System.err.println("Can not determine the path pf the MonteCarloScheduler Jar");
            System.exit(-1);
        }
        System.out.println("Starting the MonteCarloArea from" + JAR_PATH + "  on Mesos with master " + args[0]);
        Protos.FrameworkInfo frameworkInfo = Protos.FrameworkInfo.newBuilder()
                .setName("MonteCarloArea")
                .setUser("")
                .build();
        MesosSchedulerDriver schedulerDriver = new MesosSchedulerDriver(new MonteCarloScheduler(Arrays.copyOfRange(args, 2, args.length), Integer.parseInt(args[1]), JAR_PATH), frameworkInfo, args[0]);
        schedulerDriver.run();
    }
}
