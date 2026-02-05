#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Парсер расписания для извлечения свободных слотов записи
"""

import json
import csv
from datetime import datetime, timedelta
from collections import defaultdict


def parse_date(date_str):
	"""Парсинг даты в формате DD.MM.YYYY"""
	return datetime.strptime(date_str, "%d.%m.%Y")


def generate_time_slots(time_begin, time_end, slot_duration_minutes):
	"""
	Генерация всех временных слотов в интервале

	Args:
		time_begin: время начала в формате "HH:MM"
		time_end: время окончания в формате "HH:MM"
		slot_duration_minutes: длительность слота в минутах

	Returns:
		list: список времен в формате "HH:MM"
	"""
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

	Args:
		date: datetime объект

	Returns:
		str: номер дня недели как строка
	"""
	# Python: 0=понедельник, 6=воскресенье
	# Система: 1=понедельник, 7=воскресенье
	return str(date.weekday() + 1)


def parse_schedule(json_file_path, current_datetime=None):
	"""
	Основная функция парсинга расписания

	Args:
		json_file_path: путь к JSON файлу
		current_datetime: текущие дата и время (если None, используется datetime.now())

	Returns:
		list: список словарей со свободными слотами
	"""
	with open(json_file_path, 'r', encoding='utf-8') as f:
		data = json.load(f)

	response = data.get('response', {})
	times = response.get('times', [])
	busy = response.get('busy', [])
	resources = response.get('resources', [])

	# Определяем текущее время
	if current_datetime is None:
		current_datetime = datetime.now()

	print(f"Текущее время: {current_datetime.strftime('%d.%m.%Y %H:%M')}")

	# Получаем информацию о ресурсе
	resource_info = {}
	if resources:
		res = resources[0]
		resource_info = {
			'doctor': res.get('EMP_NAME', ''),
			'specialty': res.get('EMP_SPEC', ''),
			'cabinet': res.get('CAB_NAME', ''),
			'department': res.get('DEP_NAME', ''),
			'start_date': parse_date(res.get('START_DATE', '')),
			'record_period': int(res.get('RECORD_PERIOD', 14))
		}

	# Создаем словарь расписания по дням недели
	schedule_by_day = defaultdict(list)

	for time_entry in times:
		day_number = time_entry.get('DAY_NUMBER')
		time_begin = time_entry.get('TIME_BEGIN_S')
		time_end = time_entry.get('TIME_END_S')
		slot_duration = int(time_entry.get('RTIME_PRIM', 30))

		schedule_by_day[day_number].append({
			'time_begin': time_begin,
			'time_end': time_end,
			'slot_duration': slot_duration
		})

	# Создаем множество занятых слотов (только со статусом "0" - занято)
	busy_slots = set()

	for busy_entry in busy:
		# SERV_STATUS "0" означает занято
		if busy_entry.get('SERV_STATUS') == '0':
			date_str = busy_entry.get('REC_DATE_DAY')
			hour = busy_entry.get('REC_DATE_HOUR')
			minute = busy_entry.get('REC_DATE_MIN')

			# Форматируем время с ведущими нулями
			time_slot = f"{hour.zfill(2)}:{minute.zfill(2)}"
			slot_key = f"{date_str} {time_slot}"
			busy_slots.add(slot_key)

	# Генерируем все возможные слоты и находим свободные
	free_slots = []

	# Определяем диапазон дат для проверки
	# Используем период записи из настроек
	record_period = resource_info.get('record_period', 14)

	# Начинаем с текущей даты
	current_date = current_datetime.date()
	current_date = datetime.combine(current_date, datetime.min.time())

	# Конечная дата = текущая дата + период записи
	end_date = current_date + timedelta(days=record_period)

	print(f"Период записи: {record_period} дней")
	print(f"Диапазон дат: {current_date.strftime('%d.%m.%Y')} - {end_date.strftime('%d.%m.%Y')}\n")

	# Проходим по всем датам в диапазоне
	while current_date <= end_date:
		day_of_week = get_day_of_week(current_date)
		date_str = current_date.strftime("%d.%m.%Y")

		# Проверяем, есть ли расписание для этого дня недели
		if day_of_week in schedule_by_day:
			for schedule_entry in schedule_by_day[day_of_week]:
				# Генерируем все слоты для этого временного интервала
				slots = generate_time_slots(
					schedule_entry['time_begin'],
					schedule_entry['time_end'],
					schedule_entry['slot_duration']
				)

				# Проверяем каждый слот
				for time_slot in slots:
					slot_key = f"{date_str} {time_slot}"

					# Создаём datetime объект для слота
					slot_datetime = datetime.strptime(slot_key, "%d.%m.%Y %H:%M")

					# Пропускаем прошедшие слоты
					if slot_datetime <= current_datetime:
						continue

					# Если слот не в списке занятых, он свободен
					if slot_key not in busy_slots:
						free_slots.append({
							'date': date_str,
							'time': time_slot,
							'datetime': slot_key,
							'day_of_week': current_date.strftime('%A'),
							'doctor': resource_info.get('doctor', ''),
							'cabinet': resource_info.get('cabinet', ''),
							'department': resource_info.get('department', '')
						})

		current_date += timedelta(days=1)

	# Сортируем по дате и времени
	free_slots.sort(key=lambda x: (parse_date(x['date']), x['time']))

	return free_slots


def save_to_csv(free_slots, output_file):
	"""
	Сохранение свободных слотов в CSV файл

	Args:
		free_slots: список словарей со свободными слотами
		output_file: путь к выходному CSV файлу
	"""
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
	"""
	Вывод статистики по свободным слотам

	Args:
		free_slots: список словарей со свободными слотами
	"""
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
	# Входной и выходной файлы
	input_file = "raw_schedule_data.json"
	output_file = "free_slots.csv"

	print("Начало парсинга расписания...")
	print(f"Входной файл: {input_file}")

	# Текущее время (можно передать явно для тестирования)
	# Например: current_time = datetime(2026, 2, 5, 21, 33)
	current_time = datetime.now()

	# Парсим расписание
	free_slots = parse_schedule(input_file, current_time)

	# Выводим статистику
	print_summary(free_slots)

	# Сохраняем в CSV
	save_to_csv(free_slots, output_file)

	print(f"\nГотово! CSV файл сохранен: {output_file}")