package com.xorg.training.kafka.join;

public class Parser {
	private final static String COMMA = ",";

	public static Employee parseEmployee(String str) {
		System.out.println("Emp:" + str);
		String[] splits = str.split(COMMA);
		Employee emp = new Employee();
		if (splits.length == 3) {
			String firstName = splits[0];
			String lastName = splits[1];

			String locId = splits[2];

			emp.setFirstName(firstName);
			emp.setLastname(lastName);
			emp.setLocationID(locId);
		}
		return emp;

	}

	public static Location parseLocation(String str) {

		System.out.println("Location:" + str);
		String[] splits = str.split(COMMA);
		Location loc = new Location();
		if (splits.length == 2) {
			String locID = splits[0];
			String locName = splits[1];

			loc.setLocId(locID);
			loc.setLocName(locName);
		}
		return loc;
	}

}
