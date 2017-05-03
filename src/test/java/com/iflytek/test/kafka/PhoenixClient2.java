package com.iflytek.test.kafka;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

public class PhoenixClient2 {
	
	private String url = "jdbc:phoenix:192.168.59.22:2181";  
//    private String driver = "com.salesforce.phoenix.jdbc.PhoenixDriver";  
	private String driver = "org.apache.phoenix.jdbc.PhoenixDriver"; 
    private Connection connection = null;  
    private static final String TABLE_NAME = "phoenix_test_chg2";
  
    public PhoenixClient2() {  
        try {  
            Class.forName(driver);  
            connection = DriverManager.getConnection(url);  
            System.out.println("Connect HBase success..");  
        } catch (ClassNotFoundException e) {  
            e.printStackTrace();  
        } catch (SQLException e) {  
            e.printStackTrace();  
        }  
    }  
  
    public void close() {  
        if (connection != null) {  
            try {  
                connection.close();  
            } catch (Exception e) {  
            } finally {  
                connection = null;  
            }  
        }  
    }  
  
    public void createTable() {  
        String sql = "create table IF NOT EXISTS " + TABLE_NAME + "(stuid integer not null primary key,name VARCHAR,age unsigned_int,score unsigned_int,classid unsigned_int)";  
        try {  
            Statement statement = connection.createStatement();  
            statement.executeUpdate(sql);  
            connection.commit();  
            System.out.println("create table success: " + sql);  
        } catch (SQLException e) {  
            e.printStackTrace();  
        }  
    }  
  
    public void deleteTable() {  
        String sql = "drop table if exists " + TABLE_NAME;  
        try {  
            Statement statement = connection.createStatement();  
            statement.executeUpdate(sql);  
            connection.commit();  
            System.out.println("delete table success: " + sql);  
        } catch (SQLException e) {  
            e.printStackTrace();  
        }  
    }  
  
    public void insertRecord() {  
        String sql = "upsert into " + TABLE_NAME + "(stuid,name,age,score,classid) values (?,?,?,?,?)";  
        PreparedStatement statement = null;
        try {  
            statement = connection.prepareStatement(sql);  
            Random random = new Random();  
            for (int i = 1; i <= 10000; i++) {  
                statement.setInt(1,i);  
                statement.setString(2,TABLE_NAME + "_" + i);  
                statement.setInt(3, random.nextInt(18));  
                statement.setInt(4, random.nextInt(100));  
                statement.setInt(5,random.nextInt(3));  
                statement.execute();  
                if(i % 1000 == 0){
                	System.out.println("commit : " + i);
                	connection.commit(); 
                	/*try {
						if(statement != null){
							statement.close();
							connection.close();
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
                	connection = DriverManager.getConnection(url); 
                	statement = connection.prepareStatement(sql);*/
                }
            }  
            connection.commit();  
        } catch (SQLException e) {  
            e.printStackTrace();  
        }  finally{
        	/*if(statement != null){
				try {
					statement.close();
					connection.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}*/
        }
    }  
  
    public void selectRecord() {  
        String sql = "select stuid,name,age,score,classid from " + TABLE_NAME;  
        ResultSet resultSet = null;  
        try {  
            Statement statement = connection.createStatement();  
            resultSet = statement.executeQuery(sql);  
            while (resultSet != null && resultSet.next()) {  
                System.out.println("stuid: " + resultSet.getString(1) + "\tname: " + resultSet.getString(2)  
                 + "\tage: " + resultSet.getString(3) + "\tscore: " + resultSet.getString(4) + "\tclassid: " + resultSet.getString(5));  
            }  
        } catch (SQLException e) {  
            e.printStackTrace();  
        }  
    }  
  
    public static void main(String[] args) {  
        PhoenixClient2 client = new PhoenixClient2();  
        client.deleteTable();
        client.createTable();  
        client.insertRecord();  
//        client.selectRecord();  
    }  

}
