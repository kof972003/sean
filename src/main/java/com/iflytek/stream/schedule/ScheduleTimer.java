package com.iflytek.stream.schedule;

import java.util.Timer;

import com.iflytek.stream.producer.AlertMsgProducer;
import com.iflytek.stream.producer.CalculateMsgProducer;

public class ScheduleTimer {
	
	protected ScheduleTask scheduleTask;
	
	public ScheduleTimer(){
		super();
	}

	public ScheduleTimer(ScheduleTask scheduleTask) {
		super();
		this.scheduleTask = scheduleTask;
	}
	
	public void start(long delay,long period) throws Exception{
		if(scheduleTask == null){
			throw new Exception("scheduleTask is null");
		}
		Timer timer = new Timer();
		timer.schedule(scheduleTask, delay, period);
	}
	
	public static void main(String[] args){
		ScheduleTimer timer = new ScheduleTimer(new ScheduleTask(new CalculateMsgProducer()));
		try {
			timer.start(5000, 5000);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
