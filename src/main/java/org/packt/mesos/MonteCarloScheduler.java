package org.packt.mesos;

import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class MonteCarloScheduler implements Scheduler {
    private LinkedList<String> tasks;
    private int taskCounter;
    private double totalArea;

    public MonteCarloScheduler(String[] args, int numTasks){
        tasks=new LinkedList<String>();
        double  xLow=Double.parseDouble(args[1]);
        double  xHigh=Double.parseDouble(args[2]);
        double  yLow=Double.parseDouble(args[3]);
        double  yHigh=Double.parseDouble(args[4]);
        for (double x=xLow;x<xHigh;x+=(xHigh-xLow)/(numTasks/2)){
            for (double y=yLow;y<yHigh;y+=(yHigh-yLow)/(numTasks/2)) {
                tasks.add(" \" "+args[0]+" \" "+x+" "+(x+(xHigh-xLow)/(numTasks/2))+" "+y+" "+(y+(yHigh-yLow)/(numTasks/2))+" "+args[5]);
            }
        }
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
        for (Protos.Offer offer : offers) {
            if(tasks.size()>0) {
                taskCounter++;
                String task = tasks.remove();
                Protos.TaskID taskID = Protos.TaskID.newBuilder().setValue(String.valueOf(taskCounter)).build();
                System.out.println("Launching task " + taskID.getValue()+" with "+task);
                Protos.ExecutorInfo executor = Protos.ExecutorInfo.newBuilder()
                        .setExecutorId(Protos.ExecutorID.newBuilder().setValue(String.valueOf(taskCounter)))
                        .setCommand(createCommand(task))
                        .setName("MonteCarlo Executor (Java)")
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
            }
        }
    }

    private Protos.CommandInfo.Builder createCommand(String args){
        return Protos.CommandInfo.newBuilder().setValue(" java -cp /vagrant/MonteCarloArea.jar:/usr/share/java/mesos-0.20.1-shaded-protobuf.jar:/vagrant/protobuf-java-2.5.0.jar  -Djava.library.path=/usr/local/lib org.packt.mesos.MonteCarloExecutor "+args);
    }

    @Override
    public void offerRescinded(SchedulerDriver schedulerDriver, Protos.OfferID offerID) {

    }

    @Override
    public void statusUpdate(SchedulerDriver schedulerDriver, Protos.TaskStatus taskStatus) {
        System.out.println("Status update: task "+taskStatus.getTaskId().getValue()+" state is "+taskStatus.getState());
        if (taskStatus.getState().equals(Protos.TaskState.TASK_FINISHED)){
            double area = Double.parseDouble(taskStatus.getData().toStringUtf8());
            totalArea+=area;
            System.out.println("Task "+taskStatus.getTaskId().getValue()+" finished with area : "+area);
            if(tasks.size()==0){
                System.out.println("Total Area : "+totalArea);
                schedulerDriver.stop();
            }
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
        if(args.length<8){
            System.err.println("Usage: MonteCarloScheduler <Master URI>  <Number of Tasks> <Curve Expression> <xLow> <xHigh> <yLow> <yHigh> <Number of Points>");
            System.exit(-1);
        }
        System.out.println("Starting the MonteCarloArea on Mesos with master "+args[0]);
        Protos.FrameworkInfo frameworkInfo = Protos.FrameworkInfo.newBuilder()
                                                     .setName("MonteCarloArea")
                                                     .setUser("")
                                                     .build();
        MesosSchedulerDriver schedulerDriver = new MesosSchedulerDriver(new MonteCarloScheduler(Arrays.copyOfRange(args,2,args.length),Integer.parseInt(args[1])),frameworkInfo,args[0]);
        schedulerDriver.run();
    }
}
