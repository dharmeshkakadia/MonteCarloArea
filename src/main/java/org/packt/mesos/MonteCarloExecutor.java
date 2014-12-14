package org.packt.mesos;

import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.protobuf.ByteString;

public class MonteCarloExecutor implements Executor{
    Expression expression;
    double xLow;
    double xHigh;
    double yLow;
    double yHigh;
    int n;

    public MonteCarloExecutor(Expression expression, double xLow, double xHigh, double yLow, double yHigh, int n) {
        this.expression = expression;
        this.xLow = xLow;
        this.xHigh = xHigh;
        this.yLow = yLow;
        this.yHigh = yHigh;
        this.n=n;
    }

    @Override
    public void registered(ExecutorDriver executorDriver, Protos.ExecutorInfo executorInfo, Protos.FrameworkInfo frameworkInfo, Protos.SlaveInfo slaveInfo) {
        System.out.println("Registered an executor on slave " + slaveInfo.getHostname());
}

    @Override
    public void reregistered(ExecutorDriver executorDriver, Protos.SlaveInfo slaveInfo) {
        System.out.println("Re-Registered an executor on slave " + slaveInfo.getHostname());
    }

    @Override
    public void disconnected(ExecutorDriver executorDriver) {
        System.out.println("Re-Disconnected the executor on slave");
    }

    @Override
    public void launchTask(final ExecutorDriver executorDriver, final Protos.TaskInfo taskInfo) {
        System.out.println("Launching task "+taskInfo.getTaskId().getValue());
        Thread thread = new Thread() {
            @Override
            public void run(){
                //Notify the status as running
                Protos.TaskStatus status = Protos.TaskStatus.newBuilder()
                                                   .setTaskId(taskInfo.getTaskId())
                                                   .setState(Protos.TaskState.TASK_RUNNING)
                                                   .build();
                executorDriver.sendStatusUpdate(status);
                System.out.println("Running task "+taskInfo.getTaskId().getValue());

                double pointsAUC=0;
                double total=0;

                for(double x=xLow;x<=xHigh;x+=(xHigh-xLow)/n){
                    for (double y=yLow;y<=yHigh;y+=(yHigh-yLow)/n) {
                        double value = expression.evaluate(x);
                        if (value >= y) {
                            pointsAUC++;
                        }
                        total++;
                    }
                }
                //Notify the status as finish
                status = Protos.TaskStatus.newBuilder()
                                 .setTaskId(taskInfo.getTaskId())
                                 .setState(Protos.TaskState.TASK_FINISHED)
                                 .setData(ByteString.copyFrom((((xHigh - xLow) * (yHigh - yLow) * pointsAUC / total) + "").getBytes()))
                                 .build();
                executorDriver.sendStatusUpdate(status);
                System.out.println("Finished task "+taskInfo.getTaskId().getValue());
            }
        };

        thread.start();
    }

    @Override
    public void killTask(ExecutorDriver executorDriver, Protos.TaskID taskID) {
        System.out.println("Killing task " + taskID);
    }

    @Override
    public void frameworkMessage(ExecutorDriver executorDriver, byte[] bytes) {
    }

    @Override
    public void shutdown(ExecutorDriver executorDriver) {
        System.out.println("Shutting down the executor");
    }

    @Override
    public void error(ExecutorDriver executorDriver, String s) {

    }

    public static void main(String[] args) {
        MesosExecutorDriver driver = new MesosExecutorDriver(new MonteCarloExecutor(Expression.fromString(args[0]),Double.parseDouble(args[1]), Double.parseDouble(args[2]), Double.parseDouble(args[3]), Double.parseDouble(args[4]), Integer.parseInt(args[5])));
        Protos.Status status = driver.run();
        System.out.println("Driver exited with status "+status);
    }
}
