import requests
import json
from datetime import datetime, timedelta
import time
from typing import Optional, Dict, Any


class MedReg39DataCollector:
	def __init__(self):
		self.base_url = "https://medreg.gov39.ru"
		self.session = requests.Session()
		self.cache_token = "c380f2b1db917ad4e37be79d5b4e8a00a"

	def login(self, fname: str, polis_num: str) -> Optional[Dict]:
		"""Авторизация на сайте"""
		login_url = f"{self.base_url}/rpc/er/login?cache={self.cache_token}"

		login_data = {
			"fname": fname,
			"polis_num": polis_num,
			"relatives": 1,
			"payment_kind": 1,
			"home_call": 0,
			"is_web": True
		}

		try:
			response = self.session.post(
				login_url,
				json=login_data,
				headers={
					"Content-Type": "application/json",
					"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
				},
				timeout=30
			)

			if response.status_code == 200:
				print("✓ Авторизация успешна!")
				return response.json()
			else:
				print(f"✗ Ошибка авторизации: {response.status_code}")
				return None

		except Exception as e:
			print(f"✗ Ошибка при авторизации: {e}")
			return None

	def get_dentists(self) -> Optional[Dict]:
		"""Получение списка всех стоматологов"""
		dentists_url = (
			f"{self.base_url}/rpc/er/resources"
			f"?agent_ids=88145891@mis_bars"
			f"&anonym="
			f"&profile=Врач-стоматолог"
			f"&mo_type=undefined"
			f"&cache={self.cache_token}"
		)

		try:
			response = self.session.get(dentists_url, timeout=30)

			if response.status_code == 200:
				print("✓ Получен список врачей")
				return response.json()
			else:
				print(f"✗ Ошибка получения списка врачей: {response.status_code}")
				return None

		except Exception as e:
			print(f"✗ Ошибка при получении списка врачей: {e}")
			return None

	def filter_oblast_dentists(self, all_data: Dict) -> Optional[Dict]:
		"""Фильтрация врачей только из Областной стоматологической поликлиники"""
		if not all_data or all_data.get("status") != "ok":
			return None

		oblast_data = {
			"lpu_id": "",
			"lpu_name": "",
			"doctors": []
		}

		for mo in all_data["response"]["mos"]:
			if mo["id"] == "77945247":  # ID Областной стоматологической поликлиники
				oblast_data["lpu_id"] = mo["id"]
				oblast_data["lpu_name"] = mo["name"]

				for division in mo["divisions"]:
					for resource in division["resources"]:
						doctor_info = {
							"id": resource["id"],
							"name": resource["emp_fio"],
							"short_name": resource["name"],
							"department": resource["department"],
							"room": resource["room"],
							"snils": resource["snils"],
							"employer": resource["employer"],
							"blocks": resource.get("blocks", [])
						}
						oblast_data["doctors"].append(doctor_info)

				print(f"✓ Найдено врачей в Областной поликлинике: {len(oblast_data['doctors'])}")
				return oblast_data

		print("✗ Не найдена Областная стоматологическая поликлиника")
		return None

	def get_doctor_schedule(self, lpu_id: str, resource_id: str,
							date_begin: str = None, date_end: str = None) -> Optional[Dict]:
		"""Получение расписания конкретного врача"""
		if not date_begin:
			date_begin = datetime.now().strftime("%d.%m.%Y")
		if not date_end:
			date_end = (datetime.now() + timedelta(days=14)).strftime("%d.%m.%Y")

		schedule_url = (
			f"{self.base_url}/rpc/er/schedule_data"
			f"?ex_system=mis_bars"
			f"&lpu={lpu_id}"
			f"&resource={resource_id}"
			f"&date_begin={date_begin}"
			f"&date_end={date_end}"
			f"&payment_kind=1"
			f"&site_id=null"
			f"&agent_id=88145891"
			f"&cache={self.cache_token}"
		)

		try:
			response = self.session.get(
				schedule_url,
				headers={
					"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
					"Accept": "application/json, text/javascript, */*; q=0.01",
					"Referer": "https://medreg.gov39.ru/"
				},
				timeout=30
			)

			if response.status_code == 200:
				return response.json()
			else:
				print(f"  ✗ Ошибка получения расписания: {response.status_code}")
				return None

		except Exception as e:
			print(f"  ✗ Ошибка при получении расписания: {e}")
			return None

	def collect_all_data(self, date_begin: str = None, date_end: str = None,
						 delay: float = 1.0) -> Dict[str, Any]:
		"""
		Сбор всех данных по врачам Областной стоматологической поликлиники
		"""
		if not date_begin:
			date_begin = datetime.now().strftime("%d.%m.%Y")
		if not date_end:
			date_end = (datetime.now() + timedelta(days=14)).strftime("%d.%m.%Y")

		print("\n" + "=" * 80)
		print("СБОР ДАННЫХ - ОБЛАСТНАЯ СТОМАТОЛОГИЧЕСКАЯ ПОЛИКЛИНИКА")
		print("=" * 80)
		print(f"Текущая дата: {datetime.now().strftime('%d.%m.%Y %H:%M')}")
		print(f"Период сбора данных: {date_begin} - {date_end}\n")

		# 1. Получаем список всех врачей
		all_dentists_data = self.get_dentists()
		if not all_dentists_data:
			return {"error": "Не удалось получить список врачей"}

		# 2. Фильтруем врачей Областной поликлиники
		oblast_data = self.filter_oblast_dentists(all_dentists_data)
		if not oblast_data:
			return {"error": "Не найдены врачи Областной поликлиники"}

		# 3. Собираем данные по каждому врачу
		collected_data = {
			"collected_at": datetime.now().strftime("%d.%m.%Y %H:%M:%S"),
			"search_period": {
				"date_begin": date_begin,
				"date_end": date_end
			},
			"lpu_info": {
				"id": oblast_data["lpu_id"],
				"name": oblast_data["lpu_name"]
			},
			"doctors": []
		}

		total_doctors = len(oblast_data['doctors'])

		for idx, doctor in enumerate(oblast_data['doctors'], 1):
			print(f"[{idx}/{total_doctors}] Собираем данные для: {doctor['name']}")
			print(f"    Отделение: {doctor['department']}, Кабинет: {doctor['room']}")

			# Получаем расписание врача
			schedule = self.get_doctor_schedule(
				lpu_id=oblast_data['lpu_id'],
				resource_id=doctor['id'],
				date_begin=date_begin,
				date_end=date_end
			)

			doctor_data = {
				"id": doctor['id'],
				"name": doctor['name'],
				"department": doctor['department'],
				"room": doctor['room'],
				"snils": doctor['snils'],
				"employer": doctor['employer'],
				"blocks": doctor['blocks'],
				"has_schedule": bool(schedule),
				"schedule_data": schedule
			}

			if schedule:
				print(f"    ✓ Расписание получено")
				# Извлекаем основную информацию о ресурсах
				if schedule.get("response", {}).get("resources"):
					resources_count = len(schedule["response"]["resources"])
					print(f"    ✓ Ресурсы расписания: {resources_count}")

				if schedule.get("response", {}).get("times"):
					times_count = len(schedule["response"]["times"])
					print(f"    ✓ Временные слоты: {times_count}")

				if schedule.get("response", {}).get("busy"):
					busy_count = len(schedule["response"]["busy"])
					print(f"    ✓ Занятые слоты: {busy_count}")
			else:
				print(f"    ✗ Расписание не получено")

			collected_data["doctors"].append(doctor_data)

			# Задержка между запросами
			if idx < total_doctors:
				time.sleep(delay)

		return collected_data

	def save_raw_data(self, data: Dict[str, Any], filename: str = "raw_data.json"):
		"""Сохранение собранных данных в JSON файл"""
		try:
			with open(filename, 'w', encoding='utf-8') as f:
				json.dump(data, f, ensure_ascii=False, indent=2)

			print(f"\n✓ Данные сохранены в файл: {filename}")
			print(f"✓ Всего собрано данных по врачам: {len(data.get('doctors', []))}")
			return True

		except Exception as e:
			print(f"✗ Ошибка при сохранении данных: {e}")
			return False


# Основная функция
def main():
	# Конфигурация
	USER_FNAME = "Черешенко"
	USER_POLIS = "" # Обязательно нужно внести номер полиса ОМС!
	OUTPUT_FILE = "raw_schedule_data.json"

	print("=" * 80)
	print("ПАРСЕР ДАННЫХ - ОБЛАСТНАЯ СТОМАТОЛОГИЧЕСКАЯ ПОЛИКЛИНИКА")
	print("=" * 80)
	print("Цель: собрать сырые данные о расписании врачей")
	print("=" * 80)

	# Создаем сборщик данных
	collector = MedReg39DataCollector()

	# Авторизация
	print("\n🔐 Авторизация на сайте...")
	login_result = collector.login(USER_FNAME, USER_POLIS)

	if not login_result:
		print("❌ Авторизация не удалась. Завершение работы.")
		return

	# Сбор данных
	print("\n📊 Начинаем сбор данных...")
	all_data = collector.collect_all_data()

	if "error" in all_data:
		print(f"\n❌ Ошибка при сборе данных: {all_data['error']}")
		return

	# Сохранение данных
	print("\n💾 Сохраняем данные в файл...")
	if collector.save_raw_data(all_data, OUTPUT_FILE):
		print("\n" + "=" * 80)
		print("✅ СБОР ДАННЫХ ЗАВЕРШЕН УСПЕШНО!")
		print("=" * 80)
		print(f"Файл с данными: {OUTPUT_FILE}")
		print(f"Общее количество врачей: {len(all_data['doctors'])}")
		print(f"Время сбора: {all_data['collected_at']}")

		# Статистика
		doctors_with_schedule = sum(1 for d in all_data['doctors'] if d['has_schedule'])
		print(f"Врачей с расписанием: {doctors_with_schedule}")
		print(f"Врачей без расписания: {len(all_data['doctors']) - doctors_with_schedule}")

	else:
		print("\n❌ Не удалось сохранить данные")


if __name__ == "__main__":
	main()