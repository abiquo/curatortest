package com.abiquo.curatortest.listener;

import static java.lang.Thread.currentThread;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.apache.log4j.Logger;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.recipes.leader.LeaderSelector;
import com.netflix.curator.framework.recipes.leader.LeaderSelectorListener;
import com.netflix.curator.framework.state.ConnectionState;
import com.netflix.curator.retry.RetryNTimes;

public class LeaderListener implements ServletContextListener, LeaderSelectorListener
{
    protected final static Logger LOGGER = Logger.getLogger(LeaderListener.class);

    /** Zk-connection to server/s cluster. */
    private final static String ZK_SERVER = System.getProperty("zk.serverConnection", "localhost:2181");

    /** Zk-node used on the ''leaderSelector'' to synch participants. */
    private final static String LEADER_PATH = "/consumer-leader";

    /** Zk-recipe to select one participant in the cluster. @see {@link LeaderSelectorListener}. */
    private LeaderSelector leaderSelector;

    /** Zk-client connected to the cluster. */
    private CuratorFramework curatorClient;

    /** Test consumer. */
    private final Consumer consumer = new Consumer();

    public void contextInitialized(final ServletContextEvent sce)
    {
        try
        {
            curatorClient =
                CuratorFrameworkFactory
                    .newClient(ZK_SERVER, 15000, 10000, new RetryNTimes(3, 1000));
            curatorClient.start();

            LOGGER.info("Connected to " + ZK_SERVER);

            leaderSelector = new LeaderSelector(curatorClient, LEADER_PATH, this);
            leaderSelector.start();
            // return immediately, will notify if leadership is elected on
            // LeaderSelectorListener#takeLeadership
        }
        catch (Exception e)
        {
            LOGGER.error("Won't start. Not connection to zk server at " + ZK_SERVER, e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Only leader has the consumers started. This method should only return when leadership is
     * being relinquished.
     */
    public void takeLeadership(final CuratorFramework client) throws Exception
    {
        LOGGER.info("Current instance is the leader");

        try
        {
            consumer.start();
        }
        catch (Exception e)
        {
            LOGGER.error("Can't start consumers", e);
            throw e; // revoque leadership
        }

        try
        {
            // hold the leadership
            currentThread().join();
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
            // expected during ''contextDestroy''
        }

        // if interrupted by a ''stateChanged'' the handler will check if require
        // ''shutdownRegisteredConsumers''
        LOGGER.info("Current instance no longer the leader");
    }

    public void contextDestroyed(final ServletContextEvent arg0)
    {
        if (leaderSelector.hasLeadership())
        {
            LOGGER.info("Leader shutdown");
            consumer.stop();
        }

        leaderSelector.close();
    }

    /**
     * If the SUSPENDED state is reported, the instance must assume that it might no longer be the
     * leader until it receives a RECONNECTED state. If the LOST state is reported, the instance is
     * no longer the leader and its takeLeadership method should exit.
     */
    public void stateChanged(final CuratorFramework client, final ConnectionState newState)
    {
        LOGGER.info("state to - " + newState.name());

        switch (newState)
        {
            case SUSPENDED:
                if (leaderSelector.hasLeadership())
                {
                    LOGGER.info("Leader suspended, shutdown");
                    consumer.stop();
                }
                break;
            case RECONNECTED:
                // theory leaderSelector.requeue(); https://github.com/Netflix/curator/issues/24
                leaderSelector.close();
                leaderSelector = new LeaderSelector(curatorClient, LEADER_PATH, this);
                leaderSelector.start();
                break;
            case CONNECTED:
                break;
            case LOST:
                // already disconnected by SUSPENDED state
                break;
        }
    }
}
