package com.iflytek.stream.schedule;

import java.util.TimerTask;

public class ScheduleTask extends TimerTask {
	
	protected CommandService commandService;
	
	public ScheduleTask(){
		super();
	}
	
	public ScheduleTask(CommandService commandService){
		super();
		this.commandService = commandService;
	}

	@Override
	public void run() {
		commandService.execute();
	}
	
	

}
