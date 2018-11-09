package com.hncy58.kafka;

import java.sql.SQLException;

import com.hncy58.ds.DSPoolUtil;

public class TestUpdateDB {

	public static void main(String[] args) {

		String sql = "update riskcontrol.inf_customer set MODIFY_DATE = now() where MOBILE_NO BETWEEN '15911018140' and '15920098140'";

		while (true) {
			try {
				int rows = DSPoolUtil.update(sql, new Object[] {});
				System.out.println("update rows -> " + rows);
			} catch (SQLException e) {
				e.printStackTrace();
			}
			try {
				Thread.sleep(5 * 1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}
