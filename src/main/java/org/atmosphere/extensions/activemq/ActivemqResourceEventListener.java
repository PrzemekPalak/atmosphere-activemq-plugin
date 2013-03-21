package org.atmosphere.extensions.activemq;

import org.atmosphere.cpr.AtmosphereResourceEvent;
import org.atmosphere.cpr.AtmosphereResourceEventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by IntelliJ IDEA.
 * User: przemyslaw.palak
 * Date: 20.03.13
 * Time: 16:27
 */
public class ActivemqResourceEventListener implements AtmosphereResourceEventListener {
    private static final Logger logger = LoggerFactory.getLogger(ActivemqBroadcaster.class);
    private ActivemqBrodcasterContainer container;
    private static ActivemqResourceEventListener instance;

    public static ActivemqResourceEventListener getInstance(){
        if (instance == null){
            instance = new ActivemqResourceEventListener();
        }
        return instance;
    }

    private ActivemqResourceEventListener() {
        logger.trace("ActivemqResourceEventListener is created");
    }

    private void touchBroadcaster(AtmosphereResourceEvent event){
        if(event.broadcaster() instanceof ActivemqBroadcaster){
            ActivemqBroadcaster broadcaster = (ActivemqBroadcaster) event.broadcaster();
            broadcaster.touch();
            logger.trace("Touching broadcaster id: "+broadcaster.getID());
        }
    }

    public void onBroadcast(AtmosphereResourceEvent event) {
    }

    public void onPreSuspend(AtmosphereResourceEvent event) {
    }

    public void onSuspend(AtmosphereResourceEvent event) {
        touchBroadcaster(event);
    }

    public void onResume(AtmosphereResourceEvent event) {
        touchBroadcaster(event);
    }

    public void onDisconnect(AtmosphereResourceEvent event) {
        touchBroadcaster(event);
    }

    public void onThrowable(AtmosphereResourceEvent event) {
    }
}
