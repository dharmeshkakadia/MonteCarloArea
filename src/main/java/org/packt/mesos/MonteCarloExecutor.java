package org.packt.mesos;

import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos;
import com.google.protobuf.ByteString;

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

    public void registered(ExecutorDriver executorDriver, Protos.ExecutorInfo executorInfo, Protos.FrameworkInfo frameworkInfo, Protos.SlaveInfo slaveInfo) {
        System.out.println("Registered an executor on slave " + slaveInfo.getHostname());
}

    public void reregistered(ExecutorDriver executorDriver, Protos.SlaveInfo slaveInfo) {
        System.out.println("Re-Registered an executor on slave " + slaveInfo.getHostname());
    }

    public void disconnected(ExecutorDriver executorDriver) {
        System.out.println("Re-Disconnected the executor on slave");
    }

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
                double pointsUnderCurve=0;
                double totalPoints=0;

                for(double x=xLow;x<=xHigh;x+=(xHigh-xLow)/n){
                    for (double y=yLow;y<=yHigh;y+=(yHigh-yLow)/n) {
                        double value=expression.evaluate(x);
                        if (value >= y) {
                            pointsUnderCurve++;
                        }
                        totalPoints++;
                    }
                }
                double area=(xHigh - xLow)*(yHigh - yLow) * pointsUnderCurve/totalPoints; // Area of Rectangle * fraction of points under curve
                //Notify the status as finish
                status = Protos.TaskStatus.newBuilder()
                                 .setTaskId(taskInfo.getTaskId())
                                 .setState(Protos.TaskState.TASK_FINISHED)
                                 .setData(ByteString.copyFrom(Double.toString(area).getBytes()))
                                 .build();
                executorDriver.sendStatusUpdate(status);
                System.out.println("Finished task "+taskInfo.getTaskId().getValue()+ " with area : "+area);
            }
        };

        thread.start();
    }

    public void killTask(ExecutorDriver executorDriver, Protos.TaskID taskID) {
        System.out.println("Killing task " + taskID);
    }

    public void frameworkMessage(ExecutorDriver executorDriver, byte[] bytes) {
    }

    public void shutdown(ExecutorDriver executorDriver) {
        System.out.println("Shutting down the executor");
    }

    public void error(ExecutorDriver executorDriver, String s) {

    }

    public static void main(String[] args) {
        if(args.length<6){
            System.err.println("Usage: MonteCarloExecutor <Expression> <xLow> <xHigh> <yLow> <yHigh> <Number of Points>");
        }
        MesosExecutorDriver driver = new MesosExecutorDriver(new MonteCarloExecutor(Expression.fromString(args[0]),Double.parseDouble(args[1]), Double.parseDouble(args[2]), Double.parseDouble(args[3]), Double.parseDouble(args[4]), Integer.parseInt(args[5])));
        Protos.Status status = driver.run();
        System.out.println("Driver exited with status "+status);
    }
}
