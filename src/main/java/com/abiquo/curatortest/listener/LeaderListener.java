package com.abiquo.curatortest.listener;

import static java.lang.Thread.currentThread;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.api.CuratorWatcher;
import com.netflix.curator.framework.recipes.leader.LeaderSelector;
import com.netflix.curator.framework.recipes.leader.LeaderSelectorListener;
import com.netflix.curator.framework.state.ConnectionState;
import com.netflix.curator.retry.RetryNTimes;

public class LeaderListener implements ServletContextListener, LeaderSelectorListener,
    CuratorWatcher
{
    protected final static Logger LOGGER = Logger.getLogger(LeaderListener.class);

    /** Zk-connection to server/s cluster. */
    private final static String ZK_SERVER =
        System.getProperty("zk.serverConnection", "localhost:2181");

    private final static String ID = System.getProperty("id", "http://localhost/api");

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

            initializeLeaderSelector();

        }
        catch (Exception e)
        {
            LOGGER.error("Won't start. Not connection to zk server at " + ZK_SERVER, e);
            throw new RuntimeException(e);
        }
    }

    private void initializeLeaderSelector() throws Exception
    {
        leaderSelector = new LeaderSelector(curatorClient, LEADER_PATH, this);
        leaderSelector.setId(ID);
        leaderSelector.start();

        String znodePath = findLastPath();
        curatorClient.checkExists().usingWatcher(this).forPath(znodePath);
    }

    /**
     * Find the path of the last node created.
     * 
     * @throws Exception
     */
    private String findLastPath() throws Exception
    {
        List<String> child;

        do
        {
            child = curatorClient.getChildren().forPath(LEADER_PATH);
        }
        while (child == null || child.size() == 0);

        Collections.sort(child, new Comparator<String>()
        {
            // names of the znodes are like 'generateduuid'-lock-seq
            // we need to compare only the seq
            public int compare(final String o1, final String o2)
            {
                Integer seq1 = Integer.valueOf(o1.substring(o1.indexOf("lock-") + 5));
                Integer seq2 = Integer.valueOf(o2.substring(o2.indexOf("lock-") + 5));

                // we want ordered descending
                return seq2.compareTo(seq1);
            }
        });

        return LEADER_PATH + "/" + child.get(0);
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
                // do nothing: the #process(WatchedEvent event) method will create the new lead
                // selector
                break;
            case CONNECTED:
                break;
            case LOST:
                // already disconnected by SUSPENDED state
                break;
        }
    }

    /**
     * Process the NodeDeleted event.
     */
    public void process(final WatchedEvent event) throws Exception
    {
        if (event.getType() == EventType.NodeDeleted)
        {
            leaderSelector.close();

            // reinitialize the leader selector
            initializeLeaderSelector();
        }
    }
}
