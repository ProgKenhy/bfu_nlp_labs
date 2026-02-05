import requests
import json
from datetime import datetime, timedelta
import time
from typing import Optional, Dict, Any, List


class MedReg39RawDataCollector:
	def __init__(self, hospital_ids):
		self.base_url = "https://medreg.gov39.ru"
		self.hospital_ids = hospital_ids
		self.session = requests.Session()
		self.cache_token = "c380f2b1db917ad4e37be79d5b4e8a00a"

	def login(self, fname: str, polis_num: str) -> Optional[Dict]:
		"""Авторизация на сайте - только сырые данные"""
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

			return response.json()

		except Exception as e:
			print(f"Ошибка при авторизации: {e}")
			return None

	def get_dentists(self) -> Optional[Dict]:
		"""Получение списка всех стоматологов - только сырые данные"""
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
			return response.json()

		except Exception as e:
			print(f"Ошибка при получении списка врачей: {e}")
			return None

	def filter_hospitals_dentists(self, all_data: Dict) -> Optional[List[Dict]]:
		"""Фильтрация врачей из указанных госпиталей - минимальная обработка"""
		if not all_data or all_data.get("status") != "ok":
			return None

		hospital_list = []

		for mo in all_data["response"]["mos"]:
			# Проверяем, входит ли эта поликлиника в наш список
			if mo["id"] in self.hospital_ids:
				hospital_info = {
					"lpu_id": mo["id"],
					"lpu_name": mo["name"],
					"doctors": []
				}

				for division in mo["divisions"]:
					for resource in division["resources"]:
						doctor_info = {
							"id": resource["id"],
							"name": resource["emp_fio"],
							"short_name": resource["name"],
							"department": division["name"],
							"room": resource["room"],
							"snils": resource["snils"],
							"employer": resource["employer"],
							"blocks": resource.get("blocks", [])
						}
						hospital_info["doctors"].append(doctor_info)

				hospital_list.append(hospital_info)

		return hospital_list

	def get_doctor_schedule(self, lpu_id: str, resource_id: str,
							date_begin: str = None, date_end: str = None) -> Optional[Dict]:
		"""Получение расписания конкретного врача - только сырые данные"""
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

			return response.json()

		except Exception as e:
			print(f"Ошибка при получении расписания для врача {resource_id}: {e}")
			return None

	def collect_all_data(self, date_begin: str = None, date_end: str = None,
						 delay: float = 1.0) -> Dict[str, Any]:
		"""
		Сбор всех данных по врачам из указанных поликлиник
		Возвращает абсолютно сырые данные без анализа
		"""
		if not date_begin:
			date_begin = datetime.now().strftime("%d.%m.%Y")
		if not date_end:
			date_end = (datetime.now() + timedelta(days=14)).strftime("%d.%m.%Y")

		print(f"Начало сбора данных: {datetime.now().strftime('%d.%m.%Y %H:%M')}")
		print(f"Период: {date_begin} - {date_end}")
		print(f"Поликлиники: {len(self.hospital_ids)}")

		# 1. Получаем список всех врачей
		all_dentists_data = self.get_dentists()
		if not all_dentists_data:
			return {"error": "Не удалось получить список врачей"}

		# 2. Фильтруем врачей указанных поликлиник
		hospitals_data = self.filter_hospitals_dentists(all_dentists_data)
		if not hospitals_data:
			return {"error": "Не найдены врачи указанных поликлиник"}

		# 3. Собираем данные по каждой поликлинике
		collected_data = {
			"collected_at": datetime.now().strftime("%d.%m.%Y %H:%M:%S"),
			"search_period": {
				"date_begin": date_begin,
				"date_end": date_end
			},
			"hospitals": []
		}

		# Обрабатываем каждую поликлинику
		for hospital in hospitals_data:
			print(f"\nСбор данных для поликлиники: {hospital['lpu_name']}")

			hospital_info = {
				"lpu_id": hospital['lpu_id'],
				"lpu_name": hospital['lpu_name'],
				"doctors": []
			}

			total_doctors = len(hospital['doctors'])
			print(f"Найдено врачей: {total_doctors}")

			for idx, doctor in enumerate(hospital['doctors'], 1):
				print(f"  Врач {idx}/{total_doctors}: {doctor['name'][:30]}...")

				# Получаем расписание врача (сырые данные)
				schedule = self.get_doctor_schedule(
					lpu_id=hospital['lpu_id'],
					resource_id=doctor['id'],
					date_begin=date_begin,
					date_end=date_end
				)

				# Сохраняем все как есть
				doctor_data = {
					"id": doctor['id'],
					"name": doctor['name'],
					"department": doctor['department'],
					"room": doctor['room'],
					"snils": doctor['snils'],
					"employer": doctor['employer'],
					"blocks": doctor['blocks'],
					"has_schedule": schedule is not None,
					"schedule_data": schedule  # Сырые данные расписания
				}

				hospital_info["doctors"].append(doctor_data)

				# Задержка между запросами
				if idx < total_doctors:
					time.sleep(delay)

			collected_data["hospitals"].append(hospital_info)
			print(f"Завершен сбор для поликлиники: {hospital['lpu_name']}")

		return collected_data

	def save_raw_data(self, data: Dict[str, Any], filename: str = "raw_data.json"):
		"""Сохранение собранных данных в JSON файл без изменений"""
		try:
			with open(filename, 'w', encoding='utf-8') as f:
				json.dump(data, f, ensure_ascii=False, indent=2)

			# Минимальная статистика только для информации
			total_hospitals = len(data.get('hospitals', []))
			total_doctors = sum(len(hospital['doctors']) for hospital in data.get('hospitals', []))

			print(f"\nДанные сохранены в файл: {filename}")
			print(f"Всего поликлиник: {total_hospitals}")
			print(f"Всего врачей: {total_doctors}")
			print(f"Время сбора: {data.get('collected_at', 'не указано')}")

			return True

		except Exception as e:
			print(f"Ошибка при сохранении данных: {e}")
			return False


# Основная функция
def main():
	# Конфигурация
	USER_FNAME = "Черешенко"
	USER_POLIS = ""  # Обязательно нужно внести номер полиса ОМС!
	OUTPUT_FILE = "gvardeysk_data.json"

	# Список ID поликлиник (можно добавлять сколько угодно)
	HOSPITAL_IDS = [
		# "77945247",  # Областная стоматологическая поликлиника
		# "5858772243",  # Центродент
		# "75791579",  # Гурьевская
		"77965272" # Гвардейская
		# # Добавьте другие ID по необходимости
	]

	print("=" * 60)
	print("СБОР СЫРЫХ ДАННЫХ РАСПИСАНИЯ")
	print("=" * 60)
	print("Собираются абсолютно сырые данные без обработки")
	print(f"Поликлиники: {len(HOSPITAL_IDS)}")
	print("=" * 60)

	# Создаем сборщик данных
	collector = MedReg39RawDataCollector(HOSPITAL_IDS)

	# Авторизация
	print("\nАвторизация на сайте...")
	login_result = collector.login(USER_FNAME, USER_POLIS)

	if not login_result:
		print("Авторизация не удалась. Завершение работы.")
		return

	# Сбор данных
	print("\nНачало сбора сырых данных...")
	all_data = collector.collect_all_data()

	if "error" in all_data:
		print(f"\nОшибка при сборе данных: {all_data['error']}")
		return

	# Сохранение данных
	print(f"\nСохранение сырых данных в файл {OUTPUT_FILE}...")
	if collector.save_raw_data(all_data, OUTPUT_FILE):
		print("\n" + "=" * 60)
		print("СБОР ДАННЫХ ЗАВЕРШЕН")
		print("=" * 60)
		print(f"Файл: {OUTPUT_FILE}")
		print(f"Размер данных: {len(str(all_data)) // 1024} KB")
	else:
		print("\nНе удалось сохранить данные")


if __name__ == "__main__":
	main()