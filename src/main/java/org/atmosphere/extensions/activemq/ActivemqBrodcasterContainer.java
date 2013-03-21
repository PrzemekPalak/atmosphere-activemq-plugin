package org.atmosphere.extensions.activemq;

import gnu.trove.set.hash.THashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.jms.ConnectionFactory;
import java.util.Date;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Created by IntelliJ IDEA.
 * User: przemyslaw.palak
 * Date: 20.03.13
 * Time: 15:50
 */
public class ActivemqBrodcasterContainer extends TimerTask {
    private static final Logger logger = LoggerFactory.getLogger(ActivemqBroadcaster.class);
    ConnectionFactory connectionFactory;
    private String defaultTopicName = "ATMOSPHERE";
    private Set<ActivemqBroadcaster> broadcasterSet;
    //default 2 min
    private long peroid = 120000;
    //deafult 2 min
    private long expirationTimeout = 120000;
    private Timer timer;

    public ActivemqBrodcasterContainer() {
        logger.trace("Initializing ActivemqBrodcasterContainer");
        broadcasterSet = new THashSet<ActivemqBroadcaster>();
        timer = new Timer();
    }

    @PostConstruct
    public void initTimer(){
        timer.schedule(this,peroid,peroid);
    }

    @Override
    public void run() {
        logger.trace("Disposing expired broadcasters");
        long currentTs = new Date().getTime();
        Set<ActivemqBroadcaster> removedBroadcasterSet = new THashSet<ActivemqBroadcaster>();
        for(ActivemqBroadcaster broadcaster : broadcasterSet){
            if (isExpired(broadcaster,currentTs)){
                logger.debug("Broadcaster "+broadcaster.getID()+" expired, diposing");
                broadcaster.releaseExternalResources();
                broadcaster.destroy();
                removedBroadcasterSet.add(broadcaster);
            }
        }
        broadcasterSet.removeAll(removedBroadcasterSet);
    }

    private boolean isExpired(ActivemqBroadcaster broadcaster, long currentTime){
        return currentTime - broadcaster.getLastTouchTs() > expirationTimeout;
    }

    public ConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }

    public void setConnectionFactory(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    public void register(ActivemqBroadcaster activemqBroadcaster) {
        broadcasterSet.add(activemqBroadcaster);
    }

    public String getDefaultTopicName() {
        return defaultTopicName;
    }

    public void setDefaultTopicName(String defaultTopicName) {
        this.defaultTopicName = defaultTopicName;
    }

    public long getExpirationTimeout() {
        return expirationTimeout;
    }

    public void setExpirationTimeout(long expirationTimeout) {
        this.expirationTimeout = expirationTimeout;
    }

    public long getPeroid() {
        return peroid;
    }

    public void setPeroid(long peroid) {
        this.peroid = peroid;
    }
}
