package org.packt.mesos;

import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class MonteCarloScheduler implements Scheduler {
    private LinkedList<String> tasks;
    private int numTasks;
    private int tasksSubmitted;
    private int tasksCompleted;
    private double totalArea;

    public MonteCarloScheduler(String[] args, int numTasks){
        this.numTasks=numTasks;
        tasks=new LinkedList<String>();
        double xLow=Double.parseDouble(args[1]);
        double xHigh=Double.parseDouble(args[2]);
        double yLow=Double.parseDouble(args[3]);
        double yHigh=Double.parseDouble(args[4]);
        double xStep=(xHigh-xLow)/(numTasks/2);
        double yStep=(yHigh-yLow)/(numTasks/2);
        for (double x=xLow;x<xHigh;x+=xStep){
            for (double y=yLow;y<yHigh;y+=yStep) {
                tasks.add(" \" "+args[0]+" \" "+x+" "+(x+xStep)+" "+y+" "+(y+ yStep)+" "+args[5]);
            }
        }
    }

    public void registered(SchedulerDriver schedulerDriver, Protos.FrameworkID frameworkID, Protos.MasterInfo masterInfo) {
        System.out.println("Scheduler registered with id " + frameworkID.getValue());
    }

    public void reregistered(SchedulerDriver schedulerDriver, Protos.MasterInfo masterInfo) {
        System.out.println("Scheduler re-registered");
    }

    public void resourceOffers(SchedulerDriver schedulerDriver, List<Protos.Offer> offers) {
        for (Protos.Offer offer : offers) {
            if(tasks.size()>0) {
                tasksSubmitted++;
                String task = tasks.remove();
                Protos.TaskID taskID = Protos.TaskID.newBuilder().setValue(String.valueOf(tasksSubmitted)).build();
                System.out.println("Launching task " + taskID.getValue()+" on slave "+offer.getSlaveId().getValue()+" with "+task);
                Protos.ExecutorInfo executor = Protos.ExecutorInfo.newBuilder()
                        .setExecutorId(Protos.ExecutorID.newBuilder().setValue(String.valueOf(tasksSubmitted)))
                        .setCommand(createCommand(task))
                        .setName("MonteCarlo Executor (Java)")
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
            }
        }
    }

    private Protos.CommandInfo.Builder createCommand(String args){
        return Protos.CommandInfo.newBuilder().setValue("java -cp $JAR_PATH org.packt.mesos.MonteCarloExecutor "+args);
    }

    public void offerRescinded(SchedulerDriver schedulerDriver, Protos.OfferID offerID) {

    }

    public void statusUpdate(SchedulerDriver schedulerDriver, Protos.TaskStatus taskStatus) {
        System.out.println("Status update: task "+taskStatus.getTaskId().getValue()+" state is "+taskStatus.getState());
        if (taskStatus.getState().equals(Protos.TaskState.TASK_FINISHED)){
            tasksCompleted++;
            double area = Double.parseDouble(taskStatus.getData().toStringUtf8());
            totalArea+=area;
            System.out.println("Task "+taskStatus.getTaskId().getValue()+" finished with area : "+area);
        } else {
            System.out.println("Task "+taskStatus.getTaskId().getValue()+" has message "+taskStatus.getMessage());
        }
        if(tasksCompleted==numTasks){
            System.out.println("Total Area : "+totalArea);
            schedulerDriver.stop();
        }
    }

    public void frameworkMessage(SchedulerDriver schedulerDriver, Protos.ExecutorID executorID, Protos.SlaveID slaveID, byte[] bytes) {

    }

    public void disconnected(SchedulerDriver schedulerDriver) {

    }

    public void slaveLost(SchedulerDriver schedulerDriver, Protos.SlaveID slaveID) {

    }

    public void executorLost(SchedulerDriver schedulerDriver, Protos.ExecutorID executorID, Protos.SlaveID slaveID, int i) {

    }

    public void error(SchedulerDriver schedulerDriver, String message) {
        System.err.println("Error : "+message);
    }
}
