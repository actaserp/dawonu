package mes.app.plc.dto;

import java.time.LocalDateTime;
import java.util.List;

import mes.app.plc.service.PlcFreshness;

/**
 * 수집기가 살아 있는지만 보는 응답.
 *
 * 이걸 볼 화면이 어디에도 없어서 그룹 '10' 은 9개월, 그룹 '20' 은 2년 9개월
 * 수집이 멈춰 있던 걸 아무도 몰랐다. 그래서 이 엔드포인트를 가장 먼저 만든다.
 */
public class PlcHealth {

	/** 조회 시각(WAS 기준) */
	public LocalDateTime serverTime;

	/** 접속 중인 수집 DB 이름. 운영이 아니라 TEST 인지 눈으로 확인할 수 있게 내보낸다. */
	public String database;

	/** 하나라도 FRESH 가 아니면 false */
	public boolean healthy;

	public List<Collector> collectors;

	/** 수집기 그룹 하나(= PLC 장비 한 대) */
	public static class Collector {

		/** 그룹코드 '10' / '20' */
		public String group;

		public String ip;
		public String port;

		/** 읽을 접점 개수(CD_V4) */
		public Integer expectedPointCount;

		/** 수집 주기 크론 원문(CD_V7) */
		public String cron;

		/** 크론에서 산출한 주기 P(초) */
		public Long cyclePeriodSeconds;

		public LocalDateTime lastCollectedAt;
		public Long ageSeconds;
		public PlcFreshness freshness;

		/** 이 그룹이 담당하는 라인들의 마지막 적재 현황 */
		public List<Line> lines;
	}

	/** 라인 한 대(호기)의 마지막 적재 현황 */
	public static class Line {

		/** 라인코드 4자리 */
		public String line;

		public LocalDateTime lastCollectedAt;
		public Long ageSeconds;
		public PlcFreshness freshness;

		/** 이 라인으로 쌓인 총 행수 */
		public Long rowCount;
	}
}
