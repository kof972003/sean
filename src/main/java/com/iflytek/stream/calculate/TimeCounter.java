package com.iflytek.stream.calculate;

import com.iflytek.stream.schedule.CommandService;

public class TimeCounter implements CommandService {
	
	@Override
	public void execute() {
		System.out.println(DataCalculator.autoBatch());
	}

}
