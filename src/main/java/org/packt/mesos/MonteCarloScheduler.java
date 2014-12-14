package org.packt.mesos;

import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class MonteCarloScheduler implements Scheduler {
    boolean taskDone;
    String[] args;

    public MonteCarloScheduler(String[] args){
        this.args=args;
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
        if (offers.size()>0 && !taskDone) {
            Protos.Offer offer = offers.get(0);

            Protos.TaskID taskID = Protos.TaskID.newBuilder().setValue("1").build();
            System.out.println("Launching task " + taskID.getValue());
            Protos.ExecutorInfo executor = Protos.ExecutorInfo.newBuilder()
                                                   .setExecutorId(Protos.ExecutorID.newBuilder().setValue("default"))
                                                   .setCommand(createCommand(args))
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

    private Protos.CommandInfo.Builder createCommand(String[] args){
        return Protos.CommandInfo.newBuilder().setValue(" java -cp /vagrant/MonteCarloArea.jar:/usr/share/java/mesos-0.20.1-shaded-protobuf.jar:/vagrant/protobuf-java-2.5.0.jar  -Djava.library.path=/usr/local/lib org.packt.mesos.MonteCarloExecutor \" "+args[0]+" \" "+args[1]+" "+args[2] +" " +args[3]+" "+args[4]+" "+args[5]);
    }

    @Override
    public void offerRescinded(SchedulerDriver schedulerDriver, Protos.OfferID offerID) {

    }

    @Override
    public void statusUpdate(SchedulerDriver schedulerDriver, Protos.TaskStatus taskStatus) {
        System.out.println("Status udpate: task "+taskStatus.getTaskId().getValue()+" state is "+taskStatus.getState());
        if (taskStatus.getState().equals(Protos.TaskState.TASK_FINISHED)){
            double area = Double.parseDouble(taskStatus.getData().toStringUtf8());
            System.out.println("Task "+taskStatus.getTaskId().getValue()+" finished "+area);
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
        MesosSchedulerDriver schedulerDriver = new MesosSchedulerDriver(new MonteCarloScheduler(Arrays.copyOfRange(args,1,args.length)),frameworkInfo,args[0]);
        schedulerDriver.run();
    }
}
