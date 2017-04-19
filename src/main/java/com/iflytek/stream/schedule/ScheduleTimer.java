package com.iflytek.stream.schedule;

import java.util.Timer;

import com.iflytek.stream.calculate.CalDataStepStat;
import com.iflytek.stream.calculate.DataCalculator;
import com.iflytek.stream.calculate.TimeCounter;
import com.iflytek.stream.consumer.AlertMsgConsumer;
import com.iflytek.stream.consumer.CalculateMsgConsumer;
import com.iflytek.stream.consumer.MessageConsumer;
import com.iflytek.stream.producer.AlertMsgProducer;
import com.iflytek.stream.producer.CalculateMsgProducer;
import com.iflytek.stream.producer.StaticMsgProducer;

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
		timer.scheduleAtFixedRate(scheduleTask, delay, period);
	}
	
	public static void alertMsgSchedule(){
		try {
			ScheduleTimer timer = new ScheduleTimer(new ScheduleTask(new AlertMsgProducer()));
			timer.start(1000, 1000);
			MessageConsumer msgConsumer = new AlertMsgConsumer();
			msgConsumer.consume();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void calMsgSchedule(){
		try {
			//消息producer
			ScheduleTimer timer = new ScheduleTimer(new ScheduleTask(new CalculateMsgProducer()));
			timer.start(1000, 1000);
//			ScheduleTimer timer2 = new ScheduleTimer(new ScheduleTask(new DataCalculator()));
//			timer2.start(1000, 1000);
			//计时器更新
			ScheduleTimer timer3 = new ScheduleTimer(new ScheduleTask(new TimeCounter()));
			timer3.start(1000, 1000);
			//数据按step分割
			ScheduleTimer timer4 = new ScheduleTimer(new ScheduleTask(new CalDataStepStat()));
			timer4.start(1000, 5000);
			MessageConsumer msgConsumer = new CalculateMsgConsumer();
			msgConsumer.consume();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void staticMsgSchedule(){
		try {
			ScheduleTimer timer = new ScheduleTimer(new ScheduleTask(new StaticMsgProducer()));
			timer.start(1000, 5000);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args){
//		alertMsgSchedule();
		calMsgSchedule();
//		staticMsgSchedule();
	}

}
