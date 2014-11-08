package org.packt.mesos;

import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

import java.util.Collections;
import java.util.List;

public class MonteCarloScheduler implements Scheduler {
    boolean taskDone;
    String expression;
    double xLow;
    double xHigh;
    double yHigh;


    public MonteCarloScheduler(String expression, double xLow, double xHigh, double yHigh){
        this.expression=expression;
        this.xLow=xLow;
        this.xHigh=xHigh;
        this.yHigh=yHigh;
    }

    @Override
    public void registered(SchedulerDriver schedulerDriver, Protos.FrameworkID frameworkID, Protos.MasterInfo masterInfo) {
        System.out.println("Scheduler registered with id " + frameworkID.getValue());
    }

    @Override
    public void reregistered(SchedulerDriver schedulerDriver, Protos.MasterInfo masterInfo) {
        System.out.println("Scheduler re-registered");
    }

    @Override
    public void resourceOffers(SchedulerDriver schedulerDriver, List<Protos.Offer> offers) {
        //for (Protos.Offer offer : offers) {
        if (offers.size()>0 && !taskDone) {
            Protos.Offer offer = offers.get(0);

            Protos.TaskID taskID = Protos.TaskID.newBuilder().setValue("1").build();
            System.out.println("Launching task " + taskID.getValue());
            System.out.println("Computing Area for expression: "+expression);
            Protos.ExecutorInfo executor = Protos.ExecutorInfo.newBuilder()
                                                   .setExecutorId(Protos.ExecutorID.newBuilder().setValue("default"))
                                                            //.setCommand(Protos.CommandInfo.newBuilder().setValue("java JenkinsExecutor"))
                                                   .setCommand(Protos.CommandInfo.newBuilder().setValue("java -cp /home/ubuntu/MonteCarloArea.jar:/home/ubuntu/mesos-0.20.1/build/src/java/target/mesos-0.20.1.jar:/home/ubuntu/mesos-0.20.1/build/src/java/target/protobuf-java-2.5.0.jar -Djava.library.path=/home/ubuntu/mesos-0.20.1/build/src/.libs/ org.packt.mesos.MonteCarloExecutor "))
                                                   .setName("Test Executor (Java)")
                                                   .setSource("java_test")
                                                   .build();

            Protos.TaskInfo taskInfo = Protos.TaskInfo.newBuilder()
                                               .setName("MonteCarloTask-" + taskID.getValue())
                                               .setTaskId(taskID)
                                               .setExecutor(Protos.ExecutorInfo.newBuilder(executor))
                                               .addResources(Protos.Resource.newBuilder()
                                                                     .setName("cpus")
                                                                     .setType(Protos.Value.Type.SCALAR)
                                                                     .setScalar(Protos.Value.Scalar.newBuilder()
                                                                                        .setValue(1)))
                                               .addResources(Protos.Resource.newBuilder()
                                                                     .setName("mem")
                                                                     .setType(Protos.Value.Type.SCALAR)
                                                                     .setScalar(Protos.Value.Scalar.newBuilder()
                                                                                        .setValue(128)))
                                               .setSlaveId(offer.getSlaveId())
                                               .build();
            schedulerDriver.launchTasks(Collections.singletonList(offer.getId()), Collections.singletonList(taskInfo));
            taskDone=true;
        }
        //}
    }

    @Override
    public void offerRescinded(SchedulerDriver schedulerDriver, Protos.OfferID offerID) {

    }

    @Override
    public void statusUpdate(SchedulerDriver schedulerDriver, Protos.TaskStatus taskStatus) {
        System.out.println("Status udpate: task "+taskStatus.getTaskId().getValue()+" state is "+taskStatus.getState());
        if (taskStatus.getState().equals(Protos.TaskState.TASK_FINISHED)){
            System.out.println("Task "+taskStatus.getTaskId().getValue()+" finished"+taskStatus.getData().toStringUtf8());
            schedulerDriver.stop();
        } else {
            System.out.println("Task "+taskStatus.getTaskId().getValue()+" has message "+taskStatus.getMessage());
        }
    }

    @Override
    public void frameworkMessage(SchedulerDriver schedulerDriver, Protos.ExecutorID executorID, Protos.SlaveID slaveID, byte[] bytes) {

    }

    @Override
    public void disconnected(SchedulerDriver schedulerDriver) {

    }

    @Override
    public void slaveLost(SchedulerDriver schedulerDriver, Protos.SlaveID slaveID) {

    }

    @Override
    public void executorLost(SchedulerDriver schedulerDriver, Protos.ExecutorID executorID, Protos.SlaveID slaveID, int i) {

    }

    @Override
    public void error(SchedulerDriver schedulerDriver, String message) {
        System.err.println("Error : "+message);
    }


    public static void main(String[] args) {
        System.out.println("Starting the MonteCarloArea on Mesos with master "+args[0]);
        Protos.FrameworkInfo frameworkInfo = Protos.FrameworkInfo.newBuilder()
                                                     .setName("MonteCarloArea")
                                                     .setUser("")
                                                     .build();
        MesosSchedulerDriver schedulerDriver = new MesosSchedulerDriver(new MonteCarloScheduler(args[1],0,1,1),frameworkInfo,args[0]);
        schedulerDriver.run();
    }
}
