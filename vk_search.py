# VKSearch 0.1.1
# © DumbVibeCode 14.12.2025
# 

import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
import time
import json
import subprocess
import os
import tempfile
import re
from urllib.parse import quote_plus

# ------------------------------------------------------
# ЛОГГЕР
# ------------------------------------------------------
try:
    from logger import log_message
except Exception:
    def log_message(msg: str):
        print(msg)

# Флаги наличия зависимостей
SELENIUM_AVAILABLE = True
REQUESTS_AVAILABLE = True
YTDLP_AVAILABLE = True

# ------------------------------------------------------
# SELENIUM + BS4
# ------------------------------------------------------
try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from webdriver_manager.chrome import ChromeDriverManager
    from selenium.webdriver.chrome.service import Service
except Exception as e:
    SELENIUM_AVAILABLE = False
    log_message(f"ERROR: Selenium не доступен: {e}")

try:
    from bs4 import BeautifulSoup
except Exception as e:
    SELENIUM_AVAILABLE = False
    log_message(f"ERROR: BeautifulSoup (bs4) не доступен: {e}")

# ------------------------------------------------------
# requests для скачивания
# ------------------------------------------------------
try:
    import requests
except Exception as e:
    REQUESTS_AVAILABLE = False
    log_message(f"ERROR: requests не доступен: {e}")

# ------------------------------------------------------
# yt-dlp для скачивания с ВК
# ------------------------------------------------------
try:
    import yt_dlp
except Exception as e:
    YTDLP_AVAILABLE = False
    log_message(f"WARNING: yt-dlp не доступен (pip install yt-dlp): {e}")

# Глобальный инстанс
_app_instance = None
_standalone_mode = False


class VKMusicSearchApp:
    """
    - открывает vk.com в Selenium-браузере;
    - ждёт логина;
    - даёт окно поиска, парсит data-audio;
    - по ПКМ по треку можно копировать и скачивать.
    """

    def __init__(self, root: tk.Tk | tk.Toplevel, auto_open_browser: bool = True):
        self.root = root
        self.driver = None

        self.search_window: tk.Toplevel | None = None
        self.query_var: tk.StringVar | None = None
        self.count_var: tk.StringVar | None = None
        self.search_status_var: tk.StringVar | None = None
        self.progress_var: tk.DoubleVar | None = None
        self.progress_bar: ttk.Progressbar | None = None
        self.speed_var: tk.StringVar | None = None
        self.speed_label: ttk.Label | None = None
        self.batch_progress_var: tk.StringVar | None = None
        self.batch_progress_label: ttk.Label | None = None
        self.tree: ttk.Treeview | None = None
        self.btn_search: ttk.Button | None = None
        self.btn_download: ttk.Button | None = None
        
        # Для отслеживания скорости скачивания
        self._download_start_time: float = 0
        self._download_bytes: int = 0
        self._batch_download_mode: bool = False  # Флаг пакетного скачивания

        self._tree_sort_reverse: dict[str, bool] = {}

        if not SELENIUM_AVAILABLE:
            messagebox.showerror(
                "Ошибка",
                "Не найдены модули Selenium / webdriver-manager / bs4.\n\n"
                "Установи:\n"
                "  pip install selenium webdriver-manager beautifulsoup4"
            )
            return

        if auto_open_browser:
            self._open_browser_and_wait_for_login()

    # --------------------------------------------------
    # ЗАПУСК БРАУЗЕРА И ЛОГИН
    # --------------------------------------------------

    def _open_browser_and_wait_for_login(self):
        """Запускает браузер и в фоне ждёт входа в ВК."""
        def worker():
            try:
                log_message("INFO: запуск Selenium-браузера для ВК")

                options = webdriver.ChromeOptions()
                options.add_argument("--start-maximized")
                options.add_argument("--disable-blink-features=AutomationControlled")
                options.add_argument(
                    "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
                )
                
                # Включаем performance logging для перехвата сетевых запросов
                options.set_capability('goog:loggingPrefs', {'performance': 'ALL'})

                try:
                    service = Service(ChromeDriverManager().install())
                    driver = webdriver.Chrome(service=service, options=options)
                except Exception as e:
                    log_message(
                        f"WARNING: ChromeDriverManager не сработал: {e}, "
                        f"пробую webdriver.Chrome() по умолчанию"
                    )
                    driver = webdriver.Chrome(options=options)

                self.driver = driver
                driver.get("https://vk.com")
                log_message("INFO: Браузер открыт, жду логина...")

                self._wait_for_login_background()

            except Exception as e:
                log_message(f"ERROR: не удалось запустить браузер ВК: {e}")
                self.driver = None
                self._show_error_async(f"Ошибка запуска браузера ВК: {e}")

        threading.Thread(target=worker, daemon=True).start()

    def _wait_for_login_background(self):
        """Фоном проверяем, залогинен ли пользователь."""
        max_wait_sec = 300
        interval = 2
        waited = 0

        while waited < max_wait_sec:
            if self._is_logged_in():
                log_message("INFO: ВК-вход обнаружен, открываю окно поиска")
                self.root.after(0, self._show_search_window)
                return
            time.sleep(interval)
            waited += interval

        log_message("WARNING: не удалось автоматически определить логин в ВК за отведённое время")
        self.root.after(0, self._show_search_window)
        self._show_info_async(
            "Не удалось автоматически определить вход в ВК.\n"
            "Убедись, что ты залогинен в открытом окне браузера."
        )

    def _is_logged_in(self) -> bool:
        """Проверяем, что пользователь залогинен в ВК."""
        if self.driver is None:
            return False
        try:
            time.sleep(0.5)

            # Быстрая проверка по URL: на странице логина — точно не залогинены
            url = self.driver.current_url or ""
            if "/login" in url or "/join" in url or "id.vk.com" in url:
                return False

            # Надёжные data-testid атрибуты (актуальные на 2025–2026)
            testid_selectors = [
                "[data-testid='header-profile-menu']",
                "[data-testid='header-profile-menu-button']",
                "[data-testid='leftmenu']",
                "[data-testid='header-notification-button']",
            ]
            for sel in testid_selectors:
                if self.driver.find_elements(By.CSS_SELECTOR, sel):
                    log_message(f"DEBUG: _is_logged_in: найден {sel}")
                    return True

            # Проверка через JavaScript (user ID в глобальных данных страницы)
            try:
                user_id = self.driver.execute_script(
                    "try { return (window.vk && window.vk.id) || null } catch(e) { return null }"
                )
                if user_id:
                    log_message(f"DEBUG: _is_logged_in: user_id={user_id}")
                    return True
            except Exception:
                pass

            # Старые селекторы как запасной вариант
            legacy_selectors = [
                "a#top_profile_link",
                "a.top_profile_link",
                "a.TopNavBtn__profileLink",
                "div#side_bar",
                "nav.left_menu_nav_wrap",
            ]
            for sel in legacy_selectors:
                if self.driver.find_elements(By.CSS_SELECTOR, sel):
                    log_message(f"DEBUG: _is_logged_in: найден (legacy) {sel}")
                    return True

        except Exception as e:
            log_message(f"WARNING: ошибка при проверке логина: {e}")
        return False

    # --------------------------------------------------
    # ОКНО ПОИСКА
    # --------------------------------------------------

    def _show_search_window(self):
        if self.search_window is not None:
            try:
                self.search_window.lift()
                self.search_window.focus_force()
                return
            except Exception:
                self.search_window = None

        self.search_window = tk.Toplevel(self.root)
        self.search_window.title("Поиск музыки ВК")
        self.search_window.geometry("900x500")
        self.search_window.resizable(True, True)
        self.search_window.minsize(700, 400)
        self.search_window.protocol("WM_DELETE_WINDOW", self._on_search_close)

        self.search_window.lift()
        self.search_window.focus_force()

        main_frame = ttk.Frame(self.search_window, padding=10)
        main_frame.pack(fill=tk.BOTH, expand=True)

        title_label = ttk.Label(
            main_frame,
            text="🎵 Поиск музыки ВКонтакте (через браузер)",
            font=("Arial", 14, "bold")
        )
        title_label.pack(pady=(0, 10), anchor="w")

        # ---- верхняя панель ----
        search_frame = ttk.LabelFrame(main_frame, text="Параметры поиска", padding=8)
        search_frame.pack(fill=tk.X)

        ttk.Label(search_frame, text="Запрос:").grid(row=0, column=0, sticky="w")

        self.query_var = tk.StringVar()
        entry_query = ttk.Entry(search_frame, textvariable=self.query_var)
        entry_query.grid(row=0, column=1, sticky="we", padx=5)
        entry_query.bind("<Return>", lambda e: self._start_search())
        self._add_entry_context_menu(entry_query)

        ttk.Label(search_frame, text="Кол-во треков:").grid(
            row=0, column=2, padx=(10, 0), sticky="e"
        )
        self.count_var = tk.StringVar(value="30")
        entry_count = ttk.Entry(search_frame, textvariable=self.count_var, width=6)
        entry_count.grid(row=0, column=3, sticky="w")

        self.btn_search = ttk.Button(search_frame, text="Искать", command=self._start_search)
        self.btn_search.grid(row=0, column=4, padx=5)
        
        self.btn_download = ttk.Button(
            search_frame, 
            text="⬇ Скачать выбранные", 
            command=self._download_selected_tracks
        )
        self.btn_download.grid(row=0, column=5, padx=5)

        search_frame.columnconfigure(1, weight=1)

        # ---- статус и прогресс-бар ----
        status_frame = ttk.Frame(main_frame)
        status_frame.pack(fill=tk.X, pady=(5, 0))
        
        self.search_status_var = tk.StringVar(value="Готово")
        status_label = ttk.Label(
            status_frame,
            textvariable=self.search_status_var,
            foreground="green"
        )
        status_label.pack(side=tk.LEFT)
        
        # Прогресс-бар (скрыт по умолчанию)
        self.progress_var = tk.DoubleVar(value=0)
        self.progress_bar = ttk.Progressbar(
            status_frame,
            variable=self.progress_var,
            maximum=100,
            length=250,
            mode='determinate'
        )
        
        # Скорость скачивания
        self.speed_var = tk.StringVar(value="")
        self.speed_label = ttk.Label(
            status_frame,
            textvariable=self.speed_var,
            foreground="blue",
            font=("Arial", 9)
        )
        
        # Метка для общего прогресса (X/Y треков)
        self.batch_progress_var = tk.StringVar(value="")
        self.batch_progress_label = ttk.Label(
            status_frame,
            textvariable=self.batch_progress_var,
            foreground="purple",
            font=("Arial", 9, "bold")
        )

        # ---- таблица ----
        results_frame = ttk.LabelFrame(main_frame, text="Результаты поиска", padding=5)
        results_frame.pack(fill=tk.BOTH, expand=True, pady=(10, 0))

        # добавляем скрытые столбцы url и audio_full_id (для yt-dlp)
        columns = ("artist", "title", "duration", "owner", "url", "audio_full_id")
        self.tree = ttk.Treeview(
            results_frame,
            columns=columns,
            displaycolumns=("artist", "title", "duration", "owner"),
            show="headings",
            height=15,
            selectmode="extended"  # множественный выбор
        )

        self.tree.heading("artist", text="Исполнитель")
        self.tree.heading("title", text="Название")
        self.tree.heading("duration", text="Длительность")
        self.tree.heading("owner", text="Владелец")
        self.tree.heading("url", text="")
        self.tree.heading("audio_full_id", text="")

        self.tree.column("artist", width=220, anchor="w")
        self.tree.column("title", width=420, anchor="w")
        self.tree.column("duration", width=80, anchor="center")
        self.tree.column("owner", width=120, anchor="w")
        self.tree.column("url", width=0, stretch=False, anchor="w")
        self.tree.column("audio_full_id", width=0, stretch=False, anchor="w")

        vsb = ttk.Scrollbar(results_frame, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=vsb.set)

        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        vsb.pack(side=tk.RIGHT, fill=tk.Y)

        # сортировка + контекстное меню
        self._init_tree_sorting()
        self._add_tree_context_menu()

    def _on_search_close(self):
        """Закрыть окно и убить браузер."""
        try:
            if self.driver is not None:
                self.driver.quit()
        except Exception:
            pass
        self.driver = None

        if self.search_window is not None:
            self.search_window.destroy()
            self.search_window = None

        if _standalone_mode:
            try:
                self.root.quit()
                self.root.destroy()
            except Exception:
                pass

    # --------------------------------------------------
    # КОНТЕКСТНОЕ МЕНЮ ДЛЯ ENTRY
    # --------------------------------------------------

    @staticmethod
    def _entry_select_all(entry: tk.Entry | ttk.Entry):
        entry.select_range(0, "end")
        entry.icursor("end")

    def _add_entry_context_menu(self, entry: ttk.Entry):
        menu = tk.Menu(entry, tearoff=0)
        menu.add_command(label="Вырезать", command=lambda: entry.event_generate("<<Cut>>"))
        menu.add_command(label="Копировать", command=lambda: entry.event_generate("<<Copy>>"))
        menu.add_command(label="Вставить", command=lambda: entry.event_generate("<<Paste>>"))
        menu.add_separator()
        menu.add_command(label="Выделить всё", command=lambda: self._entry_select_all(entry))

        def show_menu(event):
            menu.tk_popup(event.x_root, event.y_root)

        entry.bind("<Button-3>", show_menu)
        entry.bind("<Control-a>", lambda e: (self._entry_select_all(entry), "break"))

        entry._popup_menu = menu  # чтобы не улетело

    # --------------------------------------------------
    # КОНТЕКСТНОЕ МЕНЮ ДЛЯ ТАБЛИЦЫ
    # --------------------------------------------------

    def _add_tree_context_menu(self):
        if not self.tree:
            return

        menu = tk.Menu(self.tree, tearoff=0)
        menu.add_command(
            label="Копировать «Исполнитель – Название»",
            command=self._copy_artist_title_from_row
        )
        menu.add_command(
            label="Копировать ссылку на владельца",
            command=self._copy_owner_link_from_row
        )
        menu.add_separator()
        menu.add_command(
            label="Скачать трек",
            command=self._download_track_from_row
        )
        menu.add_command(
            label="Скачать выбранные",
            command=self._download_selected_tracks
        )
        menu.add_separator()
        menu.add_command(
            label="Выбрать все",
            command=lambda: self.tree.selection_set(self.tree.get_children())
        )

        def show_menu(event):
            iid = self.tree.identify_row(event.y)
            if iid:
                # Если кликнули на невыбранную строку - выбираем только её
                # Если на выбранную - оставляем текущий выбор
                if iid not in self.tree.selection():
                    self.tree.selection_set(iid)
                menu.tk_popup(event.x_root, event.y_root)

        self.tree.bind("<Button-3>", show_menu)
        self.tree._popup_menu = menu
        
        # Горячие клавиши
        self.tree.bind("<Control-a>", lambda e: self.tree.selection_set(self.tree.get_children()))
        self.tree.bind("<Control-d>", lambda e: self._download_selected_tracks())
        self.tree.bind("<Return>", lambda e: self._download_selected_tracks())

    def _get_selected_row_values(self):
        if not self.tree:
            return None
        sel = self.tree.selection()
        if not sel:
            return None
        return self.tree.item(sel[0], "values")

    def _copy_artist_title_from_row(self):
        vals = self._get_selected_row_values()
        if not vals or len(vals) < 2:
            return
        artist = vals[0]
        title = vals[1]
        text = f"{artist} - {title}" if title else artist

        w = self.search_window or self.root
        try:
            w.clipboard_clear()
            w.clipboard_append(text)
            self._set_search_status("Исполнитель и название скопированы")
        except Exception as e:
            log_message(f"WARNING: не удалось скопировать трек в буфер: {e}")

    def _copy_owner_link_from_row(self):
        vals = self._get_selected_row_values()
        if not vals or len(vals) < 4:
            self._set_search_status("Нет данных о владельце")
            return

        owner_val = str(vals[3])
        url = None

        owner_str = owner_val.strip()

        if owner_str.startswith("id"):
            num = owner_str[2:].strip()
            if num:
                url = f"https://vk.com/id{num}"
        elif owner_str.startswith("club"):
            num = owner_str[4:].strip()
            if num:
                url = f"https://vk.com/club{num}"
        else:
            try:
                oid = int(owner_str)
                if oid < 0:
                    url = f"https://vk.com/club{abs(oid)}"
                elif oid > 0:
                    url = f"https://vk.com/id{oid}"
            except Exception:
                url = None

        if not url:
            self._set_search_status("Не удалось построить ссылку на владельца")
            return

        w = self.search_window or self.root
        try:
            w.clipboard_clear()
            w.clipboard_append(url)
            self._set_search_status("Ссылка на владельца скопирована")
        except Exception as e:
            log_message(f"WARNING: не удалось скопировать ссылку владельца: {e}")

    # ---- СКАЧИВАНИЕ ----
    def _download_track_from_row(self):
        """
        Скачивает трек. Получает m3u8 ссылку через клик в браузере,
        затем скачивает через yt-dlp.
        """
        vals = self._get_selected_row_values()
        if not vals or len(vals) < 6:
            self._set_search_status("Нет данных для скачивания")
            log_message("DOWNLOAD: нет vals или длина < 6")
            return

        artist = (vals[0] or "").strip()
        title = (vals[1] or "").strip()
        direct_url = (vals[4] or "").strip()  # прямая ссылка на mp3
        audio_full_id = (vals[5] or "").strip()  # owner_id_audio_id

        log_message(f"DOWNLOAD: artist={artist!r}, title={title!r}")
        log_message(f"DOWNLOAD: direct_url={direct_url[:80] if direct_url else 'None'}...")
        log_message(f"DOWNLOAD: audio_id={audio_full_id!r}")

        if not audio_full_id:
            self._set_search_status("Нет ID трека для скачивания")
            return

        # Безопасное имя файла
        base_name = f"{artist} - {title}".strip(" -") or "track"
        safe_name = "".join(c for c in base_name if c not in '<>:"/\\|?*')
        if not safe_name:
            safe_name = "track"

        default_filename = safe_name + ".mp3"

        path = filedialog.asksaveasfilename(
            parent=self.search_window,
            title="Сохранить трек как...",
            defaultextension=".mp3",
            initialfile=default_filename,
            filetypes=[("Аудио MP3", "*.mp3"), ("Все файлы", "*.*")]
        )
        if not path:
            self._set_search_status("Сохранение отменено")
            return

        def worker():
            success = False
            
            # Показываем прогресс-бар
            self._show_progress_bar()
            self._update_progress(0, "")

            # Способ 1: получаем m3u8 через клик в браузере и качаем через yt-dlp
            if self.driver and YTDLP_AVAILABLE:
                success = self._download_via_browser_intercept(audio_full_id, path)

            # Способ 2: прямая ссылка с cookies
            if not success and direct_url and direct_url.startswith("http"):
                success = self._download_via_direct_url(direct_url, path)

            # Скрываем прогресс-бар
            self._hide_progress_bar()
            
            if not success:
                self._set_search_status("Не удалось скачать трек")
                self._show_error_async(
                    "Не удалось скачать трек.\n\n"
                    "Возможные причины:\n"
                    "• Трек недоступен для скачивания\n"
                    "• Проблемы с авторизацией\n"
                    "• Нужен yt-dlp и ffmpeg"
                )
            else:
                self._set_search_status("✓ Трек скачан!")

        threading.Thread(target=worker, daemon=True).start()

    def _download_selected_tracks(self):
        """
        Скачивает все выбранные треки в указанную папку.
        """
        if not self.tree:
            return
        selection = self.tree.selection()
        if not selection:
            self._set_search_status("Не выбрано ни одного трека")
            return
        # Собираем данные выбранных треков
        tracks = []
        for iid in selection:
            vals = self.tree.item(iid, "values")
            if vals and len(vals) >= 6:
                artist = (vals[0] or "").strip()
                title = (vals[1] or "").strip()
                direct_url = (vals[4] or "").strip()
                audio_full_id = (vals[5] or "").strip()
                if audio_full_id:
                    tracks.append({
                        'artist': artist,
                        'title': title,
                        'direct_url': direct_url,
                        'audio_full_id': audio_full_id
                    })

        if not tracks:
            self._set_search_status("Нет треков для скачивания")
            return

        # Запрашиваем папку для сохранения
        folder = filedialog.askdirectory(
            parent=self.search_window,
            title=f"Выберите папку для сохранения ({len(tracks)} треков)"
        )
        if not folder:
            self._set_search_status("Сохранение отменено")
            return

        log_message(f"DOWNLOAD BATCH: {len(tracks)} треков в {folder}")

        def worker():
            success_count = 0
            fail_count = 0
            total = len(tracks)
            # Устанавливаем флаг пакетного режима
            self._batch_download_mode = True
            # Показываем прогресс-бар в пакетном режиме
            self._show_progress_bar(batch_mode=True)

            # --- НОВОЕ: Список неудачных треков ---
            failed_tracks_list = []
            # --- КОНЕЦ НОВОГО ---

            # --- НОВОЕ: Переменные для расчёта ETA ---
            start_time = time.time()
            time_per_track = 0
            # --- КОНЕЦ НОВОГО ---

            for i, track in enumerate(tracks, 1):
                artist = track['artist']
                title = track['title']
                audio_full_id = track['audio_full_id']
                direct_url = track['direct_url']
                # Формируем безопасное имя файла
                base_name = f"{artist} - {title}".strip(" -") or f"track_{i}"
                safe_name = "".join(c for c in base_name if c not in '<>:"/\\|?*')
                if not safe_name:
                    safe_name = f"track_{i}"
                path = os.path.join(folder, safe_name + ".mp3")
                # Если файл существует - добавляем номер
                counter = 1
                original_path = path
                while os.path.exists(path):
                    name_without_ext = original_path.rsplit('.', 1)[0]
                    path = f"{name_without_ext} ({counter}).mp3"
                    counter += 1

                # Обновляем общий прогресс (% по количеству треков)
                overall_percent = (i / total) * 100  # Обновляем до i, а не (i-1)

                # --- НОВОЕ: Расчёт ETA ---
                current_time = time.time()
                elapsed_time = current_time - start_time
                time_per_track = elapsed_time / i  # Среднее время на трек
                remaining_tracks = total - i
                eta_seconds = time_per_track * remaining_tracks
                eta_str = self._format_seconds(eta_seconds)
                # --- КОНЕЦ НОВОГО ---

                # --- НОВОЕ: Обновление прогресса напрямую ---
                # Обновляем прогресс-бар напрямую через Tkinter thread-safe метод
                self.search_window.after(0, lambda p=overall_percent, t=f"[{i}/{total}]", eta=eta_str: (
                    setattr(self.progress_bar, 'value', p), # Обновление значения напрямую
                    self.batch_progress_var.set(f"{t} ~{eta}") # Обновление метки с ETA
                ))
                # --- КОНЕЦ НОВОГО ---

                self._set_search_status(f"{safe_name[:50]}...")
                log_message(f"DOWNLOAD BATCH [{i}/{total}]: {safe_name}")

                success = False
                # Способ 1: через браузер
                if self.driver:
                    success = self._download_via_browser_intercept(audio_full_id, path)
                # Способ 2: прямая ссылка
                if not success and direct_url and direct_url.startswith("http"):
                    success = self._download_via_direct_url(direct_url, path)

                if success:
                    success_count += 1
                    log_message(f"DOWNLOAD BATCH: успешно скачан {safe_name}")
                else:
                    fail_count += 1
                    log_message(f"DOWNLOAD BATCH: не удалось скачать {safe_name}")
                    # --- НОВОЕ: Добавляем в список неудачных ---
                    failed_tracks_list.append(track)
                    # --- КОНЕЦ НОВОГО ---

                # Небольшая пауза между скачиваниями
                time.sleep(0.5)

            # Скрываем прогресс-бар и сбрасываем флаг
            self._batch_download_mode = False
            # Обновляем до 100% и индикатор через Tkinter thread-safe метод
            self.search_window.after(0, lambda t=f"[{total}/{total}]": (
                setattr(self.progress_bar, 'value', 100), # Установка значения напрямую
                self.batch_progress_var.set(t) # batch_progress_var всё ещё нужен для метки
            ))
            time.sleep(0.3)
            self._hide_progress_bar()

            # --- НОВОЕ: Сохраняем неудачные треки в файл ---
            failed_file_path = None
            if failed_tracks_list:
                failed_file_path = os.path.join(folder, "failed_tracks.json")
                try:
                    with open(failed_file_path, 'w', encoding='utf-8') as f:
                        json.dump(failed_tracks_list, f, ensure_ascii=False, indent=2)
                    log_message(f"DOWNLOAD BATCH: список неудачных треков сохранён в {failed_file_path}")
                except Exception as e:
                    log_message(f"WARNING: не удалось сохранить список неудачных треков: {e}")
            # --- КОНЕЦ НОВОГО ---

            # --- НОВОЕ: Повторные попытки для неудачных ---
            if failed_tracks_list:
                self.search_window.after(0, lambda: self._set_search_status(f"Завершено. {success_count} ок, {fail_count} не скачано. Повторные попытки..."))
                log_message("DOWNLOAD BATCH: начинаю повторные попытки для неудачных треков...")
                retry_success = 0
                retry_fail = 0
                for attempt in range(1, 3): # 2 попытки
                    log_message(f"DOWNLOAD BATCH: попытка #{attempt} для неудачных треков")
                    remaining_failed = []
                    for track in failed_tracks_list:
                        # Используем те же имена файлов, что и в основной папке
                        base_name = f"{track['artist']} - {track['title']}".strip(" -") or f"retry_track_{track['audio_full_id']}"
                        safe_name = "".join(c for c in base_name if c not in '<>:"/\\|?*')
                        if not safe_name:
                            safe_name = f"retry_track_{track['audio_full_id']}"
                        path = os.path.join(folder, safe_name + ".mp3")
                        # Если файл существует - добавляем номер (на случай, если в основной папке уже есть)
                        counter = 1
                        original_path = path
                        while os.path.exists(path):
                            name_without_ext = original_path.rsplit('.', 1)[0]
                            path = f"{name_without_ext} ({counter}).mp3"
                            counter += 1

                        log_message(f"RETRY [{attempt}/2]: {safe_name}")
                        success = False
                        if self.driver:
                             success = self._download_via_browser_intercept(track['audio_full_id'], path)
                        if not success and track['direct_url'] and track['direct_url'].startswith("http"):
                            success = self._download_via_direct_url(track['direct_url'], path)

                        if success:
                            retry_success += 1
                            log_message(f"RETRY [{attempt}/2]: успешно скачан {safe_name}")
                        else:
                            retry_fail += 1
                            remaining_failed.append(track) # Добавляем в список оставшихся
                            log_message(f"RETRY [{attempt}/2]: неудача для {safe_name}")
                    # Обновляем список неудачных для следующей итерации
                    failed_tracks_list = remaining_failed
                    if not failed_tracks_list:
                        break # Если больше нет неудачных, выходим из цикла попыток
                    time.sleep(1) # Пауза между попытками

                # Обновляем итоговый счётчик: вычитаем количество, которое удалось на повторных попытках
                fail_count = retry_fail
                success_count = total - fail_count

                # --- НОВОЕ: Пересохраняем файл с оставшимися неудачными ---
                if failed_tracks_list:
                    try:
                        with open(failed_file_path, 'w', encoding='utf-8') as f:
                            json.dump(failed_tracks_list, f, ensure_ascii=False, indent=2)
                        log_message(f"DOWNLOAD BATCH: обновлён список неудачных треков в {failed_file_path}")
                    except Exception as e:
                        log_message(f"WARNING: не удалось обновить список неудачных треков: {e}")
                else:
                    # Если все неудачные скачались, удаляем файл
                    if failed_file_path and os.path.exists(failed_file_path):
                        try:
                            os.remove(failed_file_path)
                            log_message(f"DOWNLOAD BATCH: файл с неудачными треками удалён, все скачаны")
                        except Exception as e:
                            log_message(f"WARNING: не удалось удалить файл с неудачными треками: {e}")
                # --- КОНЕЦ НОВОГО ---

            # Итоговый статус через Tkinter thread-safe метод
            if fail_count == 0:
                self.search_window.after(0, lambda: self._set_search_status(f"✓ Скачано {success_count} треков"))
                log_message(f"DOWNLOAD BATCH complete: {success_count} ok, {fail_count} failed (all retries done)")
            else:
                self.search_window.after(0, lambda: self._set_search_status(f"Скачано {success_count}, не удалось: {fail_count}. См. failed_tracks.json"))
                log_message(f"DOWNLOAD BATCH complete: {success_count} ok, {fail_count} failed. See {failed_file_path}")

        threading.Thread(target=worker, daemon=True).start()



    def _download_m3u8_silent(self, url: str, path: str) -> bool:
        """
        Скачивает аудио без обновления UI (для параллельного скачивания).
        """
        is_m3u8 = 'index.m3u8' in url or '.m3u8' in url
        
        if is_m3u8:
            try:
                output_path = path
                if output_path.lower().endswith('.mp3'):
                    output_path = output_path[:-4]

                cmd = [
                    'yt-dlp',
                    '--no-warnings',
                    '--quiet',  # Тихий режим
                    '-o', output_path + '.%(ext)s',
                    '-x',
                    '--audio-format', 'mp3',
                    '--audio-quality', '0',
                    url
                ]
                
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=180,  # 3 минуты таймаут
                    creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0
                )
                
                return result.returncode == 0
                
            except subprocess.TimeoutExpired:
                log_message(f"DOWNLOAD: таймаут для {path}")
                return False
            except Exception as e:
                log_message(f"DOWNLOAD: ошибка {e}")
                return False
        else:
            # Прямая ссылка - качаем через requests
            return self._download_via_direct_url(url, path)

    def _download_via_browser_intercept(self, audio_full_id: str, path: str) -> bool:
        """
        Кликает на трек в браузере, перехватывает m3u8 URL через Performance Log,
        затем скачивает через yt-dlp.
        """
        try:
            self._set_search_status("Получаю ссылку на аудио...")
            log_message(f"DOWNLOAD intercept: audio_id={audio_full_id}")

            # Ищем элемент трека на странице и кликаем
            m3u8_url = self._get_audio_url_via_click(audio_full_id)
            
            if not m3u8_url:
                log_message("DOWNLOAD intercept: не удалось получить m3u8 URL")
                return False

            log_message(f"DOWNLOAD intercept: got URL: {m3u8_url[:80]}...")

            # Скачиваем через yt-dlp
            return self._download_m3u8_via_ytdlp(m3u8_url, path)

        except Exception as e:
            log_message(f"DOWNLOAD intercept failed: {e}")
            return False

    def _get_audio_url_via_click(self, audio_full_id: str) -> str | None:
        """
        Кликает на трек и получает URL аудио из сетевых запросов.
        """
        if not self.driver:
            return None

        try:
            # Включаем перехват сетевых запросов через CDP
            self.driver.execute_cdp_cmd('Network.enable', {})
            
            # Очищаем историю запросов
            self.driver.execute_cdp_cmd('Network.clearBrowserCache', {})

            # Ищем элемент трека по audio_full_id
            # Формат: data-full-id="owner_id_audio_id" или class содержит audio_row
            selector = f'div.audio_row[data-full-id="{audio_full_id}"]'
            
            try:
                audio_element = self.driver.find_element(By.CSS_SELECTOR, selector)
            except Exception:
                # Пробуем найти по data-audio атрибуту
                rows = self.driver.find_elements(By.CSS_SELECTOR, 'div.audio_row')
                audio_element = None
                for row in rows:
                    try:
                        data_audio = row.get_attribute('data-audio')
                        if data_audio and audio_full_id in data_audio:
                            audio_element = row
                            break
                    except Exception:
                        continue
                
                if not audio_element:
                    log_message(f"DOWNLOAD: не найден элемент трека {audio_full_id}")
                    return None

            # Кликаем на кнопку воспроизведения
            try:
                play_btn = audio_element.find_element(By.CSS_SELECTOR, '.audio_play_wrap, .audio_row__play_btn, .audio_row__cover')
                self.driver.execute_script("arguments[0].click();", play_btn)
            except Exception:
                # Кликаем на сам элемент
                self.driver.execute_script("arguments[0].click();", audio_element)

            log_message("DOWNLOAD: кликнули на трек, ждём загрузки URL...")

            # Быстрое ожидание URL - проверяем каждые 0.3 сек, максимум 2 сек
            audio_url = None
            for _ in range(7):  # 7 * 0.3 = 2.1 сек максимум
                time.sleep(0.3)
                
                # Получаем URL через JavaScript - проверяем разные места
                audio_url = self.driver.execute_script("""
                    // Способ 1: глобальный плеер VK (новый)
                    try {
                        if (window.ap && window.ap._impl) {
                            var impl = window.ap._impl;
                            if (impl._currentAudio && impl._currentAudio.url) {
                                return impl._currentAudio.url;
                            }
                            if (impl.currentAudio && impl.currentAudio.url) {
                                return impl.currentAudio.url;
                            }
                        }
                    } catch(e) {}
                    
                    // Способ 2: getAudioPlayer()
                    try {
                        if (typeof getAudioPlayer === 'function') {
                            var player = getAudioPlayer();
                            if (player) {
                                if (player._impl && player._impl._currentAudio) {
                                    return player._impl._currentAudio.url;
                                }
                                if (player.getCurrentAudio) {
                                    var audio = player.getCurrentAudio();
                                    if (audio && audio.url) return audio.url;
                                }
                            }
                        }
                    } catch(e) {}
                    
                    // Способ 3: HTML5 audio элемент
                    try {
                        var audioEl = document.querySelector('audio');
                        if (audioEl && audioEl.src && audioEl.src.length > 10) {
                            return audioEl.src;
                        }
                    } catch(e) {}
                    
                    // Способ 4: через AudioPlayer глобальный объект
                    try {
                        if (window.AudioPlayer && window.AudioPlayer.prototype) {
                            var instances = Object.values(window).filter(function(v) {
                                return v && v.constructor && v.constructor.name === 'AudioPlayer';
                            });
                            for (var i = 0; i < instances.length; i++) {
                                if (instances[i]._currentAudio && instances[i]._currentAudio.url) {
                                    return instances[i]._currentAudio.url;
                                }
                            }
                        }
                    } catch(e) {}
                    
                    // Способ 5: через cur.audioPlayer
                    try {
                        if (window.cur && window.cur.audioPlayer) {
                            var ap = window.cur.audioPlayer;
                            if (ap._impl && ap._impl._currentAudio) {
                                return ap._impl._currentAudio.url;
                            }
                        }
                    } catch(e) {}
                    
                    return null;
                """)
                
                if audio_url:
                    log_message(f"DOWNLOAD: URL получен за {(_ + 1) * 0.3:.1f} сек")
                    break

            # Останавливаем воспроизведение
            try:
                self.driver.execute_script("""
                    try {
                        if (window.ap && window.ap.pause) window.ap.pause();
                        if (typeof getAudioPlayer === 'function') {
                            var p = getAudioPlayer();
                            if (p && p.pause) p.pause();
                        }
                        var audio = document.querySelector('audio');
                        if (audio) audio.pause();
                    } catch(e) {}
                """)
            except Exception:
                pass

            if audio_url:
                log_message(f"DOWNLOAD: получен URL из JS: {audio_url[:80]}...")
                return audio_url

            # Альтернатива: смотрим в Performance log
            try:
                logs = self.driver.get_log('performance')
                log_message(f"DOWNLOAD: получено {len(logs)} записей в performance log")
                
                m3u8_url = None
                fallback_url = None
                
                for entry in reversed(logs):  # Сначала новые
                    try:
                        msg = json.loads(entry['message'])
                        message_data = msg.get('message', {})
                        if message_data.get('method') == 'Network.requestWillBeSent':
                            url = message_data.get('params', {}).get('request', {}).get('url', '')
                            
                            # Приоритет 1: index.m3u8
                            if 'index.m3u8' in url:
                                log_message(f"DOWNLOAD: найден m3u8 URL: {url}")
                                m3u8_url = url
                                break
                            
                            # Приоритет 2: любой vkuseraudio URL (может быть сегмент)
                            if 'vkuseraudio' in url and not fallback_url:
                                fallback_url = url
                                log_message(f"DOWNLOAD: найден fallback URL: {url}")
                                
                    except Exception:
                        continue
                
                # Возвращаем лучший найденный URL
                if m3u8_url:
                    return m3u8_url
                if fallback_url:
                    # Пробуем преобразовать URL сегмента в index.m3u8
                    # URL сегмента: .../seg-1-a1.ts -> .../index.m3u8
                    if '/seg-' in fallback_url:
                        m3u8_url = fallback_url.rsplit('/seg-', 1)[0] + '/index.m3u8'
                        log_message(f"DOWNLOAD: преобразован в m3u8: {m3u8_url}")
                        return m3u8_url
                    return fallback_url
                    
            except Exception as e:
                log_message(f"WARNING: не удалось получить performance logs: {e}")

            return None

        except Exception as e:
            log_message(f"ERROR _get_audio_url_via_click: {e}")
            return None

    def _download_m3u8_via_ytdlp(self, url: str, path: str) -> bool:
        """Скачивает аудио URL через yt-dlp (subprocess) или requests."""
        
        is_m3u8 = 'index.m3u8' in url or '.m3u8' in url
        
        # Для m3u8 используем yt-dlp через subprocess (как в консоли)
        if is_m3u8:
            try:
                # Показываем прогресс-бар только если не пакетный режим
                if not self._batch_download_mode:
                    self._show_progress_bar()
                self._set_search_status("Скачиваю аудио...")
                self._update_progress(0, "")
                log_message(f"DOWNLOAD m3u8 subprocess: {url}")

                # Убираем расширение, yt-dlp добавит сам
                output_path = path
                if output_path.lower().endswith('.mp3'):
                    output_path = output_path[:-4]

                # Формируем команду как в консоли
                cmd = [
                    'yt-dlp',
                    '--no-warnings',
                    '--newline',  # Важно: каждое обновление на новой строке
                    '-o', output_path + '.%(ext)s',
                    '-x',  # extract audio
                    '--audio-format', 'mp3',
                    '--audio-quality', '0',  # лучшее качество
                    url
                ]
                
                log_message(f"DOWNLOAD cmd: {' '.join(cmd[:6])}...")

                # Запускаем процесс
                process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0
                )

                # Читаем вывод и обновляем статус
                for line in process.stdout:
                    line = line.strip()
                    if line:
                        # Логируем всё кроме строк прогресса скачивания
                        if not ('[download]' in line and '%' in line):
                            log_message(f"yt-dlp: {line[:100]}")
                            
                        if '[download]' in line and '%' in line:
                            # Парсим прогресс и скорость
                            # Форматы:
                            #   [download]  45.2% of 3.52MiB at 1.25MiB/s ETA 00:02
                            #   [download]  79.4% of ~  27.12MiB at 1.73MiB/s ETA 00:03 (frag 28/37)
                            try:
                                # Процент текущего фрагмента
                                percent_match = line.split('%')[0]
                                percent_str = percent_match.split()[-1]
                                frag_percent = float(percent_str)
                                
                                # Проверяем есть ли информация о фрагментах
                                # Формат: (frag X/Y)
                                overall_percent = frag_percent
                                frag_info = ""
                                if '(frag ' in line:
                                    try:
                                        frag_part = line.split('(frag ')[1].split(')')[0]
                                        frag_current, frag_total = frag_part.split('/')
                                        frag_current = int(frag_current)
                                        frag_total = int(frag_total)
                                        # Общий прогресс = (завершённые фрагменты + прогресс текущего) / всего
                                        overall_percent = ((frag_current - 1) + frag_percent / 100) / frag_total * 100
                                        frag_info = f" (фраг. {frag_current}/{frag_total})"
                                    except:
                                        pass
                                
                                # Размер файла
                                size = ""
                                if ' of ' in line:
                                    size_part = line.split(' of ')[1].split()[0]
                                    if size_part != '~':
                                        size = size_part
                                    elif ' of ~ ' in line:
                                        size_part = line.split(' of ~ ')[1].split()[0]
                                        size = "~" + size_part
                                
                                # Скорость
                                speed = ""
                                if ' at ' in line:
                                    at_part = line.split(' at ')[1]
                                    speed = at_part.split()[0]
                                
                                # ETA
                                eta = ""
                                if 'ETA' in line:
                                    eta = line.split('ETA')[1].strip().split()[0]
                                
                                # Обновляем UI с общим прогрессом
                                self._update_progress(overall_percent, f"{speed}" if speed else "")
                                
                                status = f"Скачиваю: {overall_percent:.1f}%"
                                if size:
                                    status += f" из {size}"
                                if eta and eta != "00:00":
                                    status += f" (осталось {eta})"
                                self._set_search_status(status)
                                
                            except Exception as e:
                                log_message(f"Parse error: {e}")
                                
                        elif '[download] 100%' in line or 'has already been downloaded' in line:
                            self._update_progress(100, "")
                            self._set_search_status("Загрузка завершена, конвертирую...")
                            
                        elif 'Destination' in line:
                            self._set_search_status("Сохраняю...")
                            
                        elif 'Post-process' in line or 'ffmpeg' in line.lower() or 'Converting' in line:
                            self._update_progress(100, "конвертация")
                            self._set_search_status("Конвертирую в MP3...")
                            
                        elif 'Deleting original file' in line:
                            self._set_search_status("Завершаю...")

                process.wait()
                
                if process.returncode == 0:
                    self._update_progress(100, "готово")
                    self._set_search_status("Скачивание завершено!")
                    log_message("DOWNLOAD m3u8 subprocess: успешно")
                    if not self._batch_download_mode:
                        time.sleep(0.5)  # Показываем 100% прогресс на полсекунды
                        self._hide_progress_bar()
                    return True
                else:
                    if not self._batch_download_mode:
                        self._hide_progress_bar()
                    log_message(f"DOWNLOAD m3u8 subprocess: код возврата {process.returncode}")
                    return False

            except FileNotFoundError:
                if not self._batch_download_mode:
                    self._hide_progress_bar()
                log_message("DOWNLOAD: yt-dlp не найден, пробуем вручную...")
                return self._download_m3u8_manually(url, path)
            except Exception as e:
                if not self._batch_download_mode:
                    self._hide_progress_bar()
                log_message(f"DOWNLOAD m3u8 subprocess failed: {e}")
                return self._download_m3u8_manually(url, path)
        
        # Для прямых ссылок качаем через requests
        else:
            try:
                self._set_search_status("Скачиваю аудио...")
                self._update_progress(0, "")
                log_message(f"DOWNLOAD direct audio: {url[:80]}...")

                # Получаем cookies из Selenium
                cookies_dict = {}
                if self.driver:
                    try:
                        for cookie in self.driver.get_cookies():
                            cookies_dict[cookie['name']] = cookie['value']
                    except Exception:
                        pass

                headers = {
                    "User-Agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/131.0.0.0 Safari/537.36"
                    ),
                    "Referer": "https://vk.com/",
                    "Accept": "*/*",
                    "Origin": "https://vk.com",
                }

                with requests.get(url, headers=headers, cookies=cookies_dict, 
                                stream=True, timeout=120) as r:
                    r.raise_for_status()
                    
                    total_size = int(r.headers.get('content-length', 0))
                    downloaded = 0
                    start_time = time.time()
                    last_update_time = start_time

                    with open(path, "wb") as f:
                        for chunk in r.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                                downloaded += len(chunk)
                                
                                current_time = time.time()
                                # Обновляем UI не чаще чем раз в 0.2 секунды
                                if current_time - last_update_time >= 0.2:
                                    last_update_time = current_time
                                    
                                    # Вычисляем прогресс
                                    if total_size > 0:
                                        percent = downloaded * 100 / total_size
                                    else:
                                        percent = 0
                                    
                                    # Вычисляем скорость
                                    elapsed = current_time - start_time
                                    if elapsed > 0:
                                        speed_bps = downloaded / elapsed
                                        if speed_bps >= 1024 * 1024:
                                            speed_str = f"{speed_bps / (1024*1024):.1f} MB/s"
                                        elif speed_bps >= 1024:
                                            speed_str = f"{speed_bps / 1024:.0f} KB/s"
                                        else:
                                            speed_str = f"{speed_bps:.0f} B/s"
                                    else:
                                        speed_str = ""
                                    
                                    # Вычисляем ETA
                                    eta_str = ""
                                    if total_size > 0 and speed_bps > 0:
                                        remaining = total_size - downloaded
                                        eta_sec = remaining / speed_bps
                                        if eta_sec < 60:
                                            eta_str = f"{int(eta_sec)}с"
                                        else:
                                            eta_str = f"{int(eta_sec // 60)}м {int(eta_sec % 60)}с"
                                    
                                    # Обновляем UI
                                    self._update_progress(percent, speed_str)
                                    
                                    # Форматируем размер
                                    if total_size > 0:
                                        if total_size >= 1024 * 1024:
                                            size_str = f"{downloaded / (1024*1024):.1f}/{total_size / (1024*1024):.1f} MB"
                                        else:
                                            size_str = f"{downloaded / 1024:.0f}/{total_size / 1024:.0f} KB"
                                        status = f"Скачиваю: {percent:.0f}% ({size_str})"
                                    else:
                                        if downloaded >= 1024 * 1024:
                                            status = f"Скачано: {downloaded / (1024*1024):.1f} MB"
                                        else:
                                            status = f"Скачано: {downloaded / 1024:.0f} KB"
                                    
                                    if eta_str:
                                        status += f" ~{eta_str}"
                                    
                                    self._set_search_status(status)

                self._update_progress(100, "готово")
                self._set_search_status("Скачивание завершено!")
                log_message("DOWNLOAD direct audio: успешно")
                return True

            except Exception as e:
                log_message(f"DOWNLOAD direct audio failed: {e}")
                return False

    def _download_m3u8_manually(self, m3u8_url: str, path: str) -> bool:
        """
        Скачивает m3u8 вручную: парсит плейлист, качает сегменты, склеивает.
        """
        try:
            self._set_search_status("Скачиваю сегменты...")
            log_message(f"DOWNLOAD m3u8 manual: {m3u8_url[:60]}...")

            cookies_dict = {}
            if self.driver:
                try:
                    for cookie in self.driver.get_cookies():
                        cookies_dict[cookie['name']] = cookie['value']
                except Exception:
                    pass

            headers = {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/131.0.0.0 Safari/537.36"
                ),
                "Referer": "https://vk.com/",
                "Origin": "https://vk.com",
            }

            # 1. Скачиваем m3u8 плейлист
            resp = requests.get(m3u8_url, headers=headers, cookies=cookies_dict, timeout=30)
            resp.raise_for_status()
            m3u8_content = resp.text
            log_message(f"DOWNLOAD: m3u8 content length: {len(m3u8_content)}")

            # 2. Парсим сегменты (.ts файлы)
            base_url = m3u8_url.rsplit('/', 1)[0] + '/'
            segments = []
            for line in m3u8_content.split('\n'):
                line = line.strip()
                if line and not line.startswith('#'):
                    if line.startswith('http'):
                        segments.append(line)
                    else:
                        segments.append(base_url + line)

            if not segments:
                log_message("DOWNLOAD: не найдено сегментов в m3u8")
                return False

            log_message(f"DOWNLOAD: найдено {len(segments)} сегментов")

            # 3. Скачиваем все сегменты и склеиваем
            all_data = bytearray()
            for i, seg_url in enumerate(segments):
                self._set_search_status(f"Скачиваю сегмент {i+1}/{len(segments)}...")
                try:
                    seg_resp = requests.get(seg_url, headers=headers, cookies=cookies_dict, timeout=60)
                    seg_resp.raise_for_status()
                    all_data.extend(seg_resp.content)
                except Exception as e:
                    log_message(f"WARNING: не удалось скачать сегмент {i+1}: {e}")

            if not all_data:
                log_message("DOWNLOAD: не удалось скачать сегменты")
                return False

            # 4. Сохраняем как .ts файл
            ts_path = path.rsplit('.', 1)[0] + '.ts'
            with open(ts_path, 'wb') as f:
                f.write(all_data)

            log_message(f"DOWNLOAD: сохранено {len(all_data)} байт в {ts_path}")

            # 5. Пробуем конвертировать в mp3 через ffmpeg
            try:
                import subprocess
                mp3_path = path if path.lower().endswith('.mp3') else path.rsplit('.', 1)[0] + '.mp3'
                
                self._set_search_status("Конвертирую в MP3...")
                result = subprocess.run(
                    ['ffmpeg', '-y', '-i', ts_path, '-acodec', 'libmp3lame', '-q:a', '0', mp3_path],
                    capture_output=True,
                    timeout=120
                )
                
                if result.returncode == 0:
                    # Удаляем .ts файл
                    try:
                        os.remove(ts_path)
                    except Exception:
                        pass
                    self._set_search_status("Скачивание завершено!")
                    log_message(f"DOWNLOAD: конвертировано в {mp3_path}")
                    return True
                else:
                    log_message(f"DOWNLOAD: ffmpeg error: {result.stderr.decode()[:200]}")
                    self._set_search_status(f"Сохранено как .ts (ffmpeg недоступен)")
                    return True  # Всё равно успех, файл есть
                    
            except FileNotFoundError:
                log_message("DOWNLOAD: ffmpeg не найден, оставляем .ts")
                self._set_search_status(f"Сохранено как .ts (установи ffmpeg для MP3)")
                return True
            except Exception as e:
                log_message(f"DOWNLOAD: ошибка конвертации: {e}")
                self._set_search_status(f"Сохранено как .ts")
                return True

        except Exception as e:
            log_message(f"DOWNLOAD m3u8 manual failed: {e}")
            return False

    def _download_via_direct_url(self, url: str, path: str) -> bool:
        """Скачивает по прямой ссылке с cookies из Selenium."""
        try:
            self._set_search_status("Скачиваю (прямая ссылка)...")
            log_message(f"DOWNLOAD direct: {url[:80]}...")

            # Получаем cookies из Selenium
            cookies_dict = {}
            if self.driver:
                try:
                    for cookie in self.driver.get_cookies():
                        cookies_dict[cookie['name']] = cookie['value']
                except Exception as e:
                    log_message(f"WARNING: не удалось получить cookies: {e}")

            headers = {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                "Referer": "https://vk.com/",
                "Accept": "*/*",
                "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
            }

            with requests.get(url, headers=headers, cookies=cookies_dict, 
                            stream=True, timeout=60) as r:
                r.raise_for_status()
                
                # Проверяем content-type
                content_type = r.headers.get('content-type', '')
                if 'audio' not in content_type and 'octet-stream' not in content_type:
                    log_message(f"DOWNLOAD direct: неожиданный content-type: {content_type}")
                    if 'text/html' in content_type:
                        log_message("DOWNLOAD direct: получили HTML вместо аудио")
                        return False

                total_size = int(r.headers.get('content-length', 0))
                downloaded = 0

                with open(path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            downloaded += len(chunk)
                            if total_size > 0:
                                percent = int(downloaded * 100 / total_size)
                                self._set_search_status(f"Скачиваю: {percent}%")

            self._set_search_status("Скачивание завершено!")
            log_message("DOWNLOAD direct: успешно")
            return True

        except Exception as e:
            log_message(f"DOWNLOAD direct failed: {e}")
            return False

    def _export_cookies_for_ytdlp(self) -> str | None:
        """
        Экспортирует cookies из Selenium в формате Netscape для yt-dlp.
        Возвращает путь к временному файлу или None.
        """
        if self.driver is None:
            log_message("COOKIES: driver не доступен")
            return None

        try:
            cookies = self.driver.get_cookies()
            if not cookies:
                log_message("COOKIES: нет cookies в браузере")
                return None

            # Создаём временный файл
            fd, cookies_path = tempfile.mkstemp(suffix='.txt', prefix='vk_cookies_')
            
            with os.fdopen(fd, 'w') as f:
                # Заголовок Netscape cookies
                f.write("# Netscape HTTP Cookie File\n")
                f.write("# https://curl.haxx.se/rfc/cookie_spec.html\n")
                f.write("# This is a generated file! Do not edit.\n\n")

                for cookie in cookies:
                    domain = cookie.get('domain', '')
                    # Netscape формат требует точку в начале для поддоменов
                    if not domain.startswith('.'):
                        domain = '.' + domain
                    
                    flag = "TRUE" if domain.startswith('.') else "FALSE"
                    path = cookie.get('path', '/')
                    secure = "TRUE" if cookie.get('secure', False) else "FALSE"
                    expiry = str(int(cookie.get('expiry', 0)))
                    name = cookie.get('name', '')
                    value = cookie.get('value', '')

                    line = f"{domain}\t{flag}\t{path}\t{secure}\t{expiry}\t{name}\t{value}\n"
                    f.write(line)

            log_message(f"COOKIES: экспортировано {len(cookies)} cookies в {cookies_path}")
            return cookies_path

        except Exception as e:
            log_message(f"ERROR: не удалось экспортировать cookies: {e}")
            return None

    def _ytdlp_progress_hook(self, d):
        """Хук для отображения прогресса скачивания yt-dlp."""
        if d['status'] == 'downloading':
            percent = d.get('_percent_str', '?%').strip()
            speed = d.get('_speed_str', '').strip()
            self._set_search_status(f"Скачиваю: {percent} {speed}")
        elif d['status'] == 'finished':
            self._set_search_status("Конвертирую в MP3...")

    # --------------------------------------------------
    # СОРТИРОВКА ТАБЛИЦЫ
    # --------------------------------------------------

    def _init_tree_sorting(self):
        if not self.tree:
            return

        self._tree_sort_reverse = {col: False for col in self.tree["columns"]}

        for col in self.tree["columns"]:
            if col == "url":  # скрытый столбец не сортируем
                continue
            heading_text = self.tree.heading(col, "text")

            def _make_sort(colname, text=heading_text):
                return lambda: self._sort_tree_by_column(colname)

            self.tree.heading(col, text=heading_text, command=_make_sort(col))

    def _sort_tree_by_column(self, col: str):
        if not self.tree:
            return

        data = [(self.tree.set(item, col), item) for item in self.tree.get_children("")]

        if col == "duration":
            def key_func(v: str):
                try:
                    parts = v.split(":")
                    if len(parts) == 2:
                        return int(parts[0]) * 60 + int(parts[1])
                    return int(v)
                except Exception:
                    return 0
        else:
            def key_func(v: str):
                return (v or "").lower()

        reverse = self._tree_sort_reverse.get(col, False)
        data.sort(key=lambda x: key_func(x[0]), reverse=reverse)

        for index, (_, item_id) in enumerate(data):
            self.tree.move(item_id, "", index)

        self._tree_sort_reverse[col] = not reverse

    # --------------------------------------------------
    # ПОИСК
    # --------------------------------------------------

    def _set_search_status(self, text: str):
        if self.search_window is None:
            return

        def _upd():
            self.search_status_var.set(text)

        self.search_window.after(0, _upd)

    def _show_progress_bar(self, batch_mode: bool = False):
        """Показывает прогресс-бар и скорость."""
        if self.search_window is None:
            return
        
        def _do():
            if batch_mode and self.batch_progress_label:
                self.batch_progress_label.pack(side=tk.RIGHT, padx=(10, 0))
            if self.speed_label:
                self.speed_label.pack(side=tk.RIGHT, padx=(5, 0))
            if self.progress_bar:
                self.progress_bar.pack(side=tk.RIGHT, padx=(10, 0))
        
        self.search_window.after(0, _do)

    def _hide_progress_bar(self):
        """Скрывает прогресс-бар и скорость."""
        if self.search_window is None:
            return
        
        def _do():
            if self.progress_bar:
                self.progress_bar.pack_forget()
            if self.speed_label:
                self.speed_label.pack_forget()
            if self.batch_progress_label:
                self.batch_progress_label.pack_forget()
            if self.progress_var:
                self.progress_var.set(0)
            if self.speed_var:
                self.speed_var.set("")
            if self.batch_progress_var:
                self.batch_progress_var.set("")
        
        self.search_window.after(0, _do)

    def _update_progress(self, percent: float, speed_text: str = "", batch_text: str | None = None):
        """Обновляет прогресс-бар, скорость и batch прогресс."""
        if self.search_window is None:
            return
        
        def _do():
            if self.progress_var:
                self.progress_var.set(percent)
            if self.speed_var:
                self.speed_var.set(speed_text)
            # batch_text обновляем только если явно передан (не None)
            if self.batch_progress_var and batch_text is not None:
                self.batch_progress_var.set(batch_text)
        
        self.search_window.after(0, _do)

    @staticmethod
    def _format_seconds(seconds: float) -> str:
        """Форматирует секунды в строку MM:SS или HH:MM:SS."""
        seconds = int(seconds)
        if seconds < 0:
            return "00:00"
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        secs = seconds % 60
        if hours > 0:
            return f"{hours:02d}:{minutes:02d}:{secs:02d}"
        else:
            return f"{minutes:02d}:{secs:02d}"

    def _start_search(self):
        if self.driver is None:
            messagebox.showwarning(
                "Нет браузера",
                "Браузер ВК не запущен. Подожди, пока откроется и залогинься."
            )
            return

        query = (self.query_var.get() or "").strip()
        if not query:
            messagebox.showwarning("Пустой запрос", "Введите исполнителя или название.")
            return

        try:
            count = int((self.count_var.get() or "").strip())
        except ValueError:
            count = 30

        # 0 — "загрузить всё, что получится"
        if count < 0:
            count = 0
        if count > 500:
            count = 500

        for item in self.tree.get_children():
            self.tree.delete(item)

        if self.btn_search:
            self.btn_search.config(state=tk.DISABLED)

        # Проверяем, является ли запрос URL-ом профиля/группы ВК
        vk_profile = self._parse_vk_profile_url(query)
        if vk_profile:
            self._set_search_status(f"Загружаю музыку с {vk_profile}...")
            threading.Thread(
                target=self._load_profile_music_worker, 
                args=(vk_profile, count), 
                daemon=True
            ).start()
        else:
            self._set_search_status("Ищу музыку в ВК...")
            threading.Thread(target=self._search_worker, args=(query, count), daemon=True).start()

    def _parse_vk_profile_url(self, text: str) -> str | None:
        """
        Проверяет, является ли текст URL-ом профиля/группы ВК.
        Возвращает короткое имя или ID профиля, либо None.
        
        Примеры:
          https://vk.com/durov -> durov
          vk.com/id1 -> id1
          https://vk.com/club12345 -> club12345
          https://vk.com/public12345 -> public12345
        """
        
        # Убираем пробелы
        text = text.strip()
        
        # Паттерн для VK URL
        # Поддерживаем: https://vk.com/xxx, http://vk.com/xxx, vk.com/xxx
        pattern = r'^(?:https?://)?(?:www\.)?vk\.com/([a-zA-Z0-9._]+)(?:\?.*)?$'
        match = re.match(pattern, text)
        
        if match:
            profile_id = match.group(1)
            # Исключаем служебные страницы
            excluded = {'audio', 'audios', 'music', 'feed', 'im', 'friends', 
                       'groups', 'photos', 'video', 'docs', 'settings', 'login'}
            if profile_id.lower() not in excluded:
                return profile_id
        
        return None

    def _load_profile_music_worker(self, profile_id: str, count: int):
        """
        Загружает музыку с профиля пользователя или группы.
        """
        try:
            # Сначала переходим на страницу профиля, чтобы получить числовой ID
            profile_url = f"https://vk.com/{profile_id}"
            log_message(f"DEBUG: открываю профиль: {profile_url}")
            self.driver.get(profile_url)
            time.sleep(2)
            
            # Пытаемся получить числовой ID из страницы
            numeric_id = None
            
            # Способ 1: из URL (если редирект на id123 или club123)
            current_url = self.driver.current_url
            log_message(f"DEBUG: текущий URL: {current_url}")
            
            # Проверяем id пользователя
            id_match = re.search(r'vk\.com/id(\d+)', current_url)
            if id_match:
                numeric_id = id_match.group(1)
                log_message(f"DEBUG: найден user ID в URL: {numeric_id}")
            
            # Проверяем club/public
            club_match = re.search(r'vk\.com/(club|public)(\d+)', current_url)
            if club_match:
                numeric_id = f"-{club_match.group(2)}"  # Группы с минусом
                log_message(f"DEBUG: найден group ID в URL: {numeric_id}")
            
            # Способ 2: ищем ID в HTML странице
            if not numeric_id:
                try:
                    # Ищем в data-атрибутах или скриптах
                    page_source = self.driver.page_source
                    
                    # Ищем паттерн "oid":123456 или "owner_id":123456
                    oid_match = re.search(r'"(?:oid|owner_id)"\s*:\s*(-?\d+)', page_source)
                    if oid_match:
                        numeric_id = oid_match.group(1)
                        log_message(f"DEBUG: найден ID в HTML: {numeric_id}")
                except Exception as e:
                    log_message(f"WARNING: ошибка при поиске ID в HTML: {e}")
            
            # Способ 3: пробуем перейти напрямую на страницу аудио с коротким именем
            if not numeric_id:
                log_message(f"DEBUG: ID не найден, пробуем audios с коротким именем")
                numeric_id = profile_id
            
            # Формируем URL страницы аудио
            audio_url = f"https://vk.com/audios{numeric_id}"
            log_message(f"DEBUG: открываю аудио: {audio_url}")
            self._set_search_status(f"Открываю аудиозаписи...")
            
            self.driver.get(audio_url)
            
            # Ждём загрузки аудио
            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "audio_row"))
                )
            except Exception as e:
                log_message(f"WARNING: не дождались audio_row: {e}")
                # Возможно, аудио скрыты или их нет
                self._set_search_status("Аудиозаписи недоступны или скрыты")
                if self.btn_search:
                    self.search_window.after(0, lambda: self.btn_search.config(state=tk.NORMAL))
                return
            
            # Скроллим и собираем треки (используем существующий метод)
            self._set_search_status("Загружаю треки...")
            results = self._scroll_and_parse_audio(count)
            
            # Отображаем результаты
            self._update_results(results)
            
        except Exception as e:
            log_message(f"ERROR _load_profile_music_worker: {e}")
            self._set_search_status(f"Ошибка: {e}")
        finally:
            if self.btn_search:
                self.search_window.after(0, lambda: self.btn_search.config(state=tk.NORMAL))

    def _scroll_and_parse_audio(self, count: int) -> list:
        """
        Скроллит страницу и собирает аудиозаписи.
        Используется для загрузки музыки с профиля/группы.
        """
        limit = count if count > 0 else None
        
        # Первичный парсинг
        html = self.driver.page_source
        results = self._parse_search_results(html, limit)
        log_message(f"INFO: после первой загрузки треков: {len(results)}")
        
        need_more = True if limit is None else (len(results) < limit)
        
        if need_more:
            scroll_pause = 1.5
            max_scrolls = 20  # Больше скроллов для профилей с большой библиотекой
            
            try:
                last_height = self.driver.execute_script("return document.body.scrollHeight")
            except Exception as e:
                log_message(f"WARNING: не удалось получить scrollHeight: {e}")
                last_height = None
            
            for i in range(max_scrolls):
                self._set_search_status(
                    f"Загружаю треки... ({len(results)}/{limit if limit is not None else '∞'})"
                )
                log_message(f"DEBUG: скролл #{i + 1}")
                
                try:
                    self.driver.execute_script(
                        "window.scrollTo(0, document.body.scrollHeight);"
                    )
                except Exception as e:
                    log_message(f"WARNING: ошибка при scrollTo: {e}")
                    break
                
                time.sleep(scroll_pause)
                
                html = self.driver.page_source
                results = self._parse_search_results(html, limit)
                log_message(f"INFO: после скролла #{i + 1} треков: {len(results)}")
                
                if limit is not None and len(results) >= limit:
                    log_message("INFO: набрали запрошенное количество, прекращаю скролл")
                    break
                
                try:
                    new_height = self.driver.execute_script("return document.body.scrollHeight")
                except Exception as e:
                    log_message(f"WARNING: не удалось получить новый scrollHeight: {e}")
                    break
                
                if last_height is not None and new_height == last_height:
                    log_message("INFO: высота страницы больше не растёт, прекращаю скролл")
                    break
                
                last_height = new_height
        
        return results

    def _search_worker(self, query: str, count: int):
        """
        1) Открываем /audio?q=...&section=search
        2) Ищем ссылку 'Показать все' с section=recoms_block
        3) Кликаем
        4) Скроллим и парсим data-audio
        """
        try:
            base_url = f"https://vk.com/audio?q={quote_plus(query)}&section=search"
            log_message(f"DEBUG: открываю базовый поиск: {base_url}")
            self.driver.get(base_url)

            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "audio_row"))
                )
            except Exception as e:
                log_message(f"WARNING: не дождались audio_row на базовом поиске: {e}")

            show_all_link = None
            try:
                links = self.driver.find_elements(By.CSS_SELECTOR, "a[href*='section=recoms_block']")
                log_message(f"DEBUG: найдено ссылок section=recoms_block: {len(links)}")
                for idx, l in enumerate(links):
                    try:
                        log_message(
                            f"DEBUG: link[{idx}] href={l.get_attribute('href')} "
                            f"text={l.text!r}"
                        )
                    except Exception:
                        pass
                if links:
                    link_with_text = None
                    for l in links:
                        try:
                            if "Показать все" in (l.text or ""):
                                link_with_text = l
                                break
                        except Exception:
                            continue
                    show_all_link = link_with_text or links[0]
                    log_message(
                        "INFO: выбрана ссылка recoms_block: "
                        f"{show_all_link.get_attribute('href')}"
                    )
                else:
                    log_message(
                        "INFO: ссылка section=recoms_block не найдена, "
                        "останемся на странице поиска"
                    )
            except Exception as e:
                log_message(f"WARNING: ошибка при поиске ссылки recoms_block: {e}")

            if show_all_link is not None:
                self._set_search_status("Открываю страницу «Показать всё»...")
                try:
                    self.driver.execute_script("arguments[0].click();", show_all_link)
                except Exception as e:
                    log_message(f"WARNING: не удалось кликнуть по recoms_block: {e}")

                try:
                    WebDriverWait(self.driver, 10).until(
                        lambda d: "section=recoms_block" in d.current_url
                    )
                    log_message(f"INFO: текущий URL после клика: {self.driver.current_url}")
                except Exception as e:
                    log_message(f"WARNING: не дождались section=recoms_block в URL: {e}")

                try:
                    WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.CLASS_NAME, "audio_row"))
                    )
                except Exception as e:
                    log_message(f"WARNING: не дождались audio_row на recoms_block: {e}")
            else:
                self._set_search_status(
                    "Работаю с основной страницей поиска (recoms_block не найден)."
                )

            html = self.driver.page_source
            limit = count if count > 0 else None
            results = self._parse_search_results(html, limit)
            log_message(f"INFO: после первой загрузки треков: {len(results)}")

            need_more = True if limit is None else (len(results) < limit)

            if need_more:
                scroll_pause = 1.5
                max_scrolls = 15

                try:
                    last_height = self.driver.execute_script("return document.body.scrollHeight")
                except Exception as e:
                    log_message(f"WARNING: не удалось получить scrollHeight: {e}")
                    last_height = None

                for i in range(max_scrolls):
                    self._set_search_status(
                        f"Догружаю результаты... "
                        f"({len(results)}/{limit if limit is not None else '∞'})"
                    )
                    log_message(f"DEBUG: скролл #{i + 1}")

                    try:
                        self.driver.execute_script(
                            "window.scrollTo(0, document.body.scrollHeight);"
                        )
                    except Exception as e:
                        log_message(f"WARNING: ошибка при scrollTo: {e}")
                        break

                    time.sleep(scroll_pause)

                    html = self.driver.page_source
                    results = self._parse_search_results(html, limit)
                    log_message(
                        f"INFO: после скролла #{i + 1} треков: {len(results)}"
                    )

                    if limit is not None and len(results) >= limit:
                        log_message(
                            "INFO: набрали запрошенное количество треков, "
                            "прекращаю скролл"
                        )
                        break

                    try:
                        new_height = self.driver.execute_script(
                            "return document.body.scrollHeight"
                        )
                    except Exception as e:
                        log_message(
                            f"WARNING: не удалось получить новый scrollHeight: {e}"
                        )
                        break

                    if last_height is not None and new_height == last_height:
                        log_message(
                            "INFO: высота страницы больше не растёт, прекращаю скролл"
                        )
                        break

                    last_height = new_height

            self._update_results(results)

        except Exception as e:
            log_message(f"ERROR: ошибка при поиске ВК: {e}")
            self._set_search_status(f"Ошибка при поиске: {e}")

        finally:
            if self.btn_search and self.search_window is not None:
                self.search_window.after(
                    0, lambda: self.btn_search.config(state=tk.NORMAL)
                )

    # --------------------------------------------------
    # ПАРСИНГ data-audio
    # --------------------------------------------------

    @staticmethod
    def _parse_search_results(html: str, max_count: int | None):
        results = []
        if not html or len(html) < 100:
            return results

        soup = BeautifulSoup(html, "html.parser")
        rows = soup.find_all(
            "div",
            class_=lambda c: c and "audio_row" in c
        )

        log_message(f"INFO: найдено блоков audio_row: {len(rows)}")

        seen_ids = set()

        for row in rows:
            try:

                                # --- НОВАЯ ПРОВЕРКА НА НЕДОСТУПНОСТЬ ---
                # Проверяем, есть ли у ряда или его дочерних элементов признак недоступности
                # Примеры классов, которые могут указывать на недоступность (нужно проверить на практике)
                unavailable_indicators = [
                    'audio_claimed',   # Пример: класс на родительском div
                ]

                is_unavailable = False
                # Проверяем сам элемент row
                row_classes = row.get('class', [])
                if any(indicator in row_classes for indicator in unavailable_indicators):
                    is_unavailable = True

                # Проверяем дочерние элементы (например, название или обложку)
                if not is_unavailable:
                    for child in row.find_all():
                        child_classes = child.get('class', [])
                        if any(indicator in child_classes for indicator in unavailable_indicators):
                            is_unavailable = True
                            break

                # Если трек недоступен, пропускаем его
                if is_unavailable:
                    log_message(f"DEBUG: пропущен недоступный трек по классу/атрибуту")
                    continue
                # --- КОНЕЦ НОВОЙ ПРОВЕРКИ ---    

                data_attr = row.get("data-audio")
                if not data_attr:
                    continue

                try:
                    data = json.loads(data_attr)
                except Exception as e:
                    log_message(f"DEBUG: не смог распарсить data-audio: {e}")
                    continue

                if len(data) < 6:
                    continue

                audio_id = str(data[0])
                owner_id = str(data[1])
                link = str(data[2]) if len(data) > 2 else ""
                if len(results) < 5:
                    log_message(f"DEBUG audio link raw: {link[:120]}")
                title_html = data[3] or ""
                artist_html = data[4] or ""
                duration_val = data[5] or 0

                # иногда вместо ссылки приходит специальный маркер
                if "audio_api_unavailable" in link:
                    continue

                title = BeautifulSoup(
                    str(title_html), "html.parser"
                ).get_text(strip=True)
                artist = BeautifulSoup(
                    str(artist_html), "html.parser"
                ).get_text(strip=True)

                if not title:
                    continue

                if "аудио доступно на vk.com" in title.lower():
                    continue

                try:
                    total_sec = int(duration_val)
                except Exception:
                    total_sec = 0

                if total_sec <= 0:
                    continue

                minutes = total_sec // 60
                seconds = total_sec % 60
                duration_str = f"{minutes}:{seconds:02d}"

                full_id = f"{owner_id}_{audio_id}"
                if full_id in seen_ids:
                    continue
                seen_ids.add(full_id)

                # человекочитаемый owner
                try:
                    owner_int = int(owner_id)
                except ValueError:
                    owner_int = 0

                if owner_int < 0:
                    owner_display = f"club{abs(owner_int)}"
                else:
                    owner_display = f"id{owner_int}" if owner_int != 0 else owner_id

                results.append((
                    (artist or "Неизвестный")[:80],
                    title[:120],
                    duration_str[:10],
                    owner_display[:32],
                    link,
                    full_id,  # owner_id_audio_id для yt-dlp
                ))

                if max_count is not None and max_count > 0 and len(results) >= max_count:
                    break

            except Exception as e:
                log_message(f"DEBUG: ошибка парсинга audio_row: {e}")
                continue

        return results

    def _update_results(self, results):
        if self.search_window is None or self.tree is None:
            return

        def _do():
            self.tree.delete(*self.tree.get_children())
            for row in results:
                if len(row) >= 6:
                    artist, title, duration, owner, url, audio_full_id = row[:6]
                elif len(row) >= 5:
                    artist, title, duration, owner, url = row[:5]
                    audio_full_id = ""
                elif len(row) == 4:
                    artist, title, duration, owner = row
                    url = ""
                    audio_full_id = ""
                elif len(row) == 3:
                    artist, title, duration = row
                    owner = ""
                    url = ""
                    audio_full_id = ""
                else:
                    continue
                self.tree.insert("", tk.END, values=(artist, title, duration, owner, url, audio_full_id))

            if results:
                self.search_status_var.set(f"Найдено треков: {len(results)}")
            else:
                self.search_status_var.set("Ничего не найдено")

        self.search_window.after(0, _do)

    # --------------------------------------------------
    # ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ
    # --------------------------------------------------

    def _show_error_async(self, text: str):
        def _do():
            messagebox.showerror("Ошибка", text)
        self.root.after(0, _do)

    def _show_info_async(self, text: str):
        def _do():
            messagebox.showinfo("Информация", text)
        self.root.after(0, _do)


# ------------------------------------------------------
# ВНЕШНЯЯ ФУНКЦИЯ ДЛЯ ИНТЕГРАЦИИ
# ------------------------------------------------------
def search_vk_music(root, auto_open=True):
    """
    Использование из основной программы:
        from vk_search import search_vk_music
        ...
        search_vk_music(root)
    """
    global _app_instance
    try:
        if _app_instance is not None and _app_instance.search_window is not None:
            _app_instance.search_window.lift()
            _app_instance.search_window.focus_force()
            return

        log_message("INFO: запуск VKMusicSearchApp из search_vk_music")
        _app_instance = VKMusicSearchApp(root, auto_open_browser=auto_open)
    except Exception as e:
        log_message(f"ERROR в search_vk_music: {e}")
        messagebox.showerror("Ошибка", f"Ошибка при запуске поиска ВК: {e}")


# ------------------------------------------------------
# ОТДЕЛЬНЫЙ ЗАПУСК
# ------------------------------------------------------
if __name__ == "__main__":
    _standalone_mode = True
    root = tk.Tk()
    root.withdraw()
    search_vk_music(root, auto_open=True)
    root.mainloop()