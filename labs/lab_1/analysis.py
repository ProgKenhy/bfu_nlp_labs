#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Парсер расписания для извлечения свободных слотов записи
Поддерживает два типа расписания:
- SCH_TYPE=0: регулярное расписание по дням недели
- SCH_TYPE=2: календарное расписание по конкретным датам месяца
"""

import json
import csv
from datetime import datetime, timedelta
from collections import defaultdict


def parse_date(date_str):
	"""Парсинг даты в формате DD.MM.YYYY"""
	return datetime.strptime(date_str, "%d.%m.%Y")


def generate_time_slots(time_begin, time_end, slot_duration_minutes):
	"""Генерация всех временных слотов в интервале"""
	start_time = datetime.strptime(time_begin, "%H:%M")
	end_time = datetime.strptime(time_end, "%H:%M")

	slots = []
	current_time = start_time

	while current_time < end_time:
		slots.append(current_time.strftime("%H:%M"))
		current_time += timedelta(minutes=slot_duration_minutes)

	return slots


def get_day_of_week(date):
	"""
	Получение номера дня недели (1=понедельник, 7=воскресенье)
	"""
	return str(date.weekday() + 1)


def parse_schedule(json_file_path, current_datetime=None):
	"""Основная функция парсинга расписания"""
	with open(json_file_path, 'r', encoding='utf-8') as f:
		data = json.load(f)

	# Проверяем структуру данных
	if 'hospitals' in data:
		# Структура с hospitals
		hospitals = data.get('hospitals', [])
		if not hospitals:
			print("Нет данных о больницах")
			return []

		doctors = hospitals[0].get('doctors', [])
		if not doctors:
			print("Нет данных о врачах")
			return []

		doctor = doctors[0]
		schedule_data = doctor.get('schedule_data', {})

		if schedule_data.get('status') != 'ok':
			print("Статус расписания не 'ok'")
			return []

		response = schedule_data.get('response', {})
		search_period = data.get('search_period', {})

	elif 'response' in data:
		# Простая структура
		response = data.get('response', {})
		search_period = {}
	else:
		print("Неизвестная структура JSON")
		return []

	times = response.get('times', [])
	busy = response.get('busy', [])
	resources = response.get('resources', [])

	# Определяем текущее время
	if current_datetime is None:
		current_datetime = datetime.now()

	print(f"Текущее время: {current_datetime.strftime('%d.%m.%Y %H:%M')}")

	# Получаем информацию о ресурсе
	resource_info = {}
	schedule_type = '0'  # по умолчанию - расписание по дням недели

	if resources:
		res = resources[0]
		schedule_type = res.get('SCH_TYPE', '0')
		resource_info = {
			'doctor': res.get('EMP_NAME', ''),
			'specialty': res.get('EMP_SPEC', ''),
			'cabinet': res.get('CAB_NAME', ''),
			'department': res.get('DEP_NAME', ''),
			'record_period': int(res.get('RECORD_PERIOD', 30)),
			'schedule_type': schedule_type
		}

	print(f"Тип расписания (SCH_TYPE): {schedule_type}")
	if schedule_type == '2':
		print("  → Календарное расписание (DAY_NUMBER = число месяца)")
	else:
		print("  → Недельное расписание (DAY_NUMBER = день недели)")

	# Обрабатываем расписание в зависимости от типа
	if schedule_type == '2':
		# Календарное расписание: DAY_NUMBER = число месяца
		schedule_by_date = defaultdict(list)

		for time_entry in times:
			day_number = time_entry.get('DAY_NUMBER')  # число месяца
			time_begin = time_entry.get('TIME_BEGIN_S')
			time_end = time_entry.get('TIME_END_S')
			slot_duration = int(time_entry.get('RTIME_PRIM', 30))

			if not all([day_number, time_begin, time_end]):
				continue

			schedule_by_date[day_number].append({
				'time_begin': time_begin,
				'time_end': time_end,
				'slot_duration': slot_duration
			})

		print(f"Календарное расписание по датам: {list(schedule_by_date.keys())}")
	else:
		# Недельное расписание: DAY_NUMBER = день недели
		schedule_by_day = defaultdict(list)

		for time_entry in times:
			day_number = time_entry.get('DAY_NUMBER')  # день недели
			time_begin = time_entry.get('TIME_BEGIN_S')
			time_end = time_entry.get('TIME_END_S')
			slot_duration = int(time_entry.get('RTIME_PRIM', 30))

			if not all([day_number, time_begin, time_end]):
				continue

			schedule_by_day[day_number].append({
				'time_begin': time_begin,
				'time_end': time_end,
				'slot_duration': slot_duration
			})

		print(f"Недельное расписание по дням: {list(schedule_by_day.keys())}")

	# Создаем множества для разных типов расписания
	if schedule_type == '2':
		# Для календарного расписания: свободен = есть в busy с SERV_STATUS=1
		free_slots_set = set()
		for busy_entry in busy:
			if busy_entry.get('SERV_STATUS') == '1':
				date_str = busy_entry.get('REC_DATE_DAY')
				hour = busy_entry.get('REC_DATE_HOUR')
				minute = busy_entry.get('REC_DATE_MIN')

				if not all([date_str, hour, minute]):
					continue

				time_slot = f"{hour.zfill(2)}:{minute.zfill(2)}"
				slot_key = f"{date_str} {time_slot}"
				free_slots_set.add(slot_key)

		print(f"Найдено свободных слотов (SERV_STATUS=1): {len(free_slots_set)}")
	else:
		# Для недельного расписания: занят = есть в busy с SERV_STATUS=0
		busy_slots_set = set()
		for busy_entry in busy:
			if busy_entry.get('SERV_STATUS') == '0':
				date_str = busy_entry.get('REC_DATE_DAY')
				hour = busy_entry.get('REC_DATE_HOUR')
				minute = busy_entry.get('REC_DATE_MIN')

				if not all([date_str, hour, minute]):
					continue

				time_slot = f"{hour.zfill(2)}:{minute.zfill(2)}"
				slot_key = f"{date_str} {time_slot}"
				busy_slots_set.add(slot_key)

		print(f"Найдено занятых слотов (SERV_STATUS=0): {len(busy_slots_set)}")

	# Генерируем все возможные слоты и находим свободные
	free_slots = []

	# Определяем диапазон дат для проверки
	if search_period:
		start_date = parse_date(search_period.get('date_begin', '05.02.2026'))
		end_date = parse_date(search_period.get('date_end', '30.02.2026'))
		print(f"Используем search_period из JSON")
	else:
		record_period = resource_info.get('record_period', 30)
		start_date = current_datetime.replace(hour=0, minute=0, second=0, microsecond=0)
		end_date = start_date + timedelta(days=record_period)
		print(f"Используем record_period: {record_period} дней")

	print(f"Диапазон дат: {start_date.strftime('%d.%m.%Y')} - {end_date.strftime('%d.%m.%Y')}\n")

	# Проходим по всем датам в диапазоне
	current_date = start_date
	while current_date <= end_date:
		date_str = current_date.strftime("%d.%m.%Y")
		day_name = current_date.strftime('%A')

		# Выбираем расписание в зависимости от типа
		if schedule_type == '2':
			# Календарное расписание: используем число месяца
			day_of_month = str(current_date.day)
			schedule_entries = schedule_by_date.get(day_of_month, [])
		else:
			# Недельное расписание: используем день недели
			day_of_week = get_day_of_week(current_date)
			schedule_entries = schedule_by_day.get(day_of_week, [])

		# Обрабатываем расписание для этой даты
		for schedule_entry in schedule_entries:
			# Генерируем все слоты для этого временного интервала
			slots = generate_time_slots(
				schedule_entry['time_begin'],
				schedule_entry['time_end'],
				schedule_entry['slot_duration']
			)

			# Проверяем каждый слот
			for time_slot in slots:
				slot_key = f"{date_str} {time_slot}"
				slot_datetime = datetime.strptime(slot_key, "%d.%m.%Y %H:%M")

				# Пропускаем прошедшие слоты
				if slot_datetime <= current_datetime:
					continue

				# Логика зависит от типа расписания
				is_free = False
				if schedule_type == '2':
					# Календарное: свободен если есть в busy с SERV_STATUS=1
					is_free = slot_key in free_slots_set
				else:
					# Недельное: свободен если НЕТ в busy или есть с SERV_STATUS!=0
					is_free = slot_key not in busy_slots_set

				if is_free:
					free_slots.append({
						'date': date_str,
						'time': time_slot,
						'datetime': slot_key,
						'day_of_week': day_name,
						'doctor': resource_info.get('doctor', ''),
						'cabinet': resource_info.get('cabinet', ''),
						'department': resource_info.get('department', '')
					})

		current_date += timedelta(days=1)

	# Сортируем по дате и времени
	free_slots.sort(key=lambda x: (parse_date(x['date']), x['time']))

	print(f"Найдено свободных слотов: {len(free_slots)}")
	return free_slots


def save_to_csv(free_slots, output_file):
	"""Сохранение свободных слотов в CSV файл"""
	if not free_slots:
		print("Нет свободных слотов для сохранения")
		return

	fieldnames = ['date', 'time', 'datetime', 'day_of_week', 'doctor', 'cabinet', 'department']

	with open(output_file, 'w', newline='', encoding='utf-8-sig') as csvfile:
		writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
		writer.writeheader()
		writer.writerows(free_slots)

	print(f"Сохранено {len(free_slots)} свободных слотов в {output_file}")


def print_summary(free_slots):
	"""Вывод статистики по свободным слотам"""
	if not free_slots:
		print("\nСвободных слотов не найдено")
		return

	print(f"\n=== НАЙДЕНО СВОБОДНЫХ СЛОТОВ: {len(free_slots)} ===\n")

	# Группировка по датам
	slots_by_date = defaultdict(list)
	for slot in free_slots:
		slots_by_date[slot['date']].append(slot['time'])

	# Вывод по датам
	for date in sorted(slots_by_date.keys(), key=lambda d: parse_date(d)):
		times = sorted(slots_by_date[date])
		date_obj = parse_date(date)
		day_name = date_obj.strftime('%A')
		print(f"{date} ({day_name}):")
		print(f"  Свободные слоты: {', '.join(times)}")
		print(f"  Всего: {len(times)} слотов\n")


if __name__ == "__main__":
	import sys

	# Входной и выходной файлы
	if len(sys.argv) > 1:
		input_file = sys.argv[1]
	else:
		input_file = "big_data.json"

	output_file = "free_slots.csv"

	print("Начало парсинга расписания...")
	print(f"Входной файл: {input_file}\n")

	# Текущее время
	current_time = datetime.now()

	# Парсим расписание
	free_slots = parse_schedule(input_file, current_time)

	# Выводим статистику
	print_summary(free_slots)

	# Сохраняем в CSV
	save_to_csv(free_slots, output_file)

	print(f"\nГотово! CSV файл сохранен: {output_file}")