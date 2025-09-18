import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import time
import threading


def __loading_bar_and_info__(
    start: bool, number_of_steps: int, total_steps: int, number_of_thread: int
) -> None:
    '''Вывод инфомрации о прогрессе выполнения программы.
    start - нужно ли вывести начальную строку;
    number_page - количество спаршенных страниц;
    total_pages - всего стнраниц, которые нужно спарсить;
    miss_count - число новостей, которые не удалось спарсить;
    whitour_whole_content - число новостей, у которых не получилось полностью спарсить контент.'''
    done = int(number_of_steps / total_steps * 100) if int(
        number_of_steps / total_steps * 100
    ) < 100 or number_of_steps == total_steps else 99
    stars = int(
        40 / 100 * done
    ) if int(20 / 100 * done) < 20 or number_of_steps == total_steps else 39
    tires = 40 - stars

    if start:
        stars = 0
        tires = 40
        done = 0

    print("thread{0} <".format(number_of_thread), end="")
    for i in range(stars):
        print("*", end="")

    for i in range(tires):
        print("-", end="")
    print("> {0}% ||| {1} / {2}".format(done, number_of_steps, total_steps))


def __getPage__(url: str, file_name: str) -> None:
    '''Получение html файла страницы.
    url - ссылка на страницу;
    file_name - имя файла, в который будет сохранена страница.'''
    r = requests.get(url=url)

    with open(file_name, "w", encoding="utf-8") as file:
        file.write(r.text)

def __parse_news__(url: str) -> (str, list[str]):
    '''Получиние полного контента новости.
    url - ссылка но новость.
    Функция возвращает полный текст новости.'''
    news_file_name = "news.html"
    __getPage__(url, news_file_name)

    with open(news_file_name, encoding="utf-8") as file:
        src = file.read()

    content = BeautifulSoup(src, "lxml").find("div", class_="main").find(
        "div", class_="post__text"
    ).text.strip()
    imgs = BeautifulSoup(src, "lxml").find("div", class_="main").find(
        "div", class_="post__text"
    ).find_all("img")

    img_links = []
    for img in imgs:
        try:
            img_links.append("https://www.hse.ru" + img.get("src"))
        except:
            continue

    return (content, img_links)

def __parse_tags__(tags_container_html) -> str:
    tags = ""

    tags_container = tags_container_html
    tag = tags_container.find("a", class_="rubric")
    while True:
        try:
            tags += tag.text.strip()
            tags += ". "

            tag = tag.find_next_sibling("a", class_="tag")
        except:
            break

    return tags


def __parse_page__(page_file_name: str, news_container: pd.DataFrame) -> None:
    '''Парсинг информации с новостной страницы: ссылка на новость + короткая информация о ней.
    page_file_name - имя файла, в который сохранён код страницы;
    news_container - таблица, в которую заносится информация о новости.
    Функция также возвращает количество новостей, которые не удалось спарсить
    и количество новостей, полный контент которых спарсить не удалось.'''
    with open(page_file_name, encoding="utf-8") as file:
        src = file.read()

    soup = BeautifulSoup(src, "lxml")

    news = soup.find("div", class_="post")
    for i in range(10):
        news_day = ""
        try:
            news_day = news.find("div", class_="post-meta__day").text.strip()
        except:
            news_day = ""

        news_month = ""
        try:
            news_month = news.find("div",
                                   class_="post-meta__month").text.strip()
        except:
            news_month = ""

        news_year = ""
        try:
            news_year = news.find("div", class_="post-meta__year").text.strip()
        except:
            news_year = ""

        news_date = news_day + "." + news_month + "." + news_year

        news_name = ""
        try:
            news_name = news.find("h2",
                                  class_="first_child").find("a").text.strip()
        except:
            news_name = ""

        news_short_content = ""
        try:
            news_short_content = news.find("p", class_="first_child"
                                          ).find_next_sibling("p").text.strip()
        except:
            news_short_content = ""

        tags = ""
        try:
            tags = __parse_tags__(news.find("div", class_="tag-set"))
        except:
            tags = ""

        link = ""
        try:
            link = news.find("h2", class_="first_child").find("a").get("href")
            if not link.startswith("https://"):
                link = 'https://www.hse.ru' + link
        except:
            link = ""

        news_content = ""
        img_links = ""
        try:
            if link.startswith("https://www.hse.ru/news/"):
                temp = __parse_news__(link)
                news_content = temp[0]
                img_links = " ".join(temp[1])
        except:
            news_content = ""

        if len(
            news_day + news_month + news_year + news_name + news_short_content +
            news_content
        ) > 0:
            news_container.loc[len(news_container.index)] = [
                link, news_date, news_name, news_short_content, news_content,
                tags, img_links
            ]

        news = news.find_next_sibling("div", class_="post")

def __shutdown__():
    '''Функция выключения компьютера.'''
    print("!!! Выключение ПК через минуту !!!")
    time.sleep(60)  # ожидание 1 минуту
    os.system("shutdown -s")

def __crawling_pages__(start: int, end: int, news_container: pd.DataFrame, num_of_thread: int) -> pd.DataFrame:
    start_ = True

    page_file_name = "page.html"

    for i in range(start, end + 1):
        try:
            __getPage__("https://www.hse.ru/news/page{0}.html".format(i), page_file_name)
            __parse_page__(page_file_name, news_container)
        except:
            continue

        __loading_bar_and_info__(start=start_, number_of_steps=(i - start + 1), total_steps=(end - start + 1), number_of_thread=num_of_thread)
        start_ = False

def crawling_pages(off_pc: bool, pages: int) -> None:
    '''Парсит полностью новостной сайт.
    off_pc - нужно ли выключать пк после завершения работы;
    pages - количество страниц, которые нужно спартсить.'''
    news_container1 = pd.DataFrame(columns=["url", "date", "title", "summary", "content", "tags", "img_links"])
    news_container2 = pd.DataFrame(columns=["url", "date", "title", "summary", "content", "tags", "img_links"])
    news_container3 = pd.DataFrame(columns=["url", "date", "title", "summary", "content", "tags", "img_links"])
    news_container4 = pd.DataFrame(columns=["url", "date", "title", "summary", "content", "tags", "img_links"])
    news_container5 = pd.DataFrame(columns=["url", "date", "title", "summary", "content", "tags", "img_links"])
    news_container6 = pd.DataFrame(columns=["url", "date", "title", "summary", "content", "tags", "img_links"])

    thread1 = threading.Thread(target=__crawling_pages__, args=(0, pages // 6, news_container1, 1))
    thread2 = threading.Thread(target=__crawling_pages__, args=(pages // 6, pages // 6 * 2, news_container2, 2))
    thread3 = threading.Thread(target=__crawling_pages__, args=(pages // 6 * 2, pages // 6 * 3, news_container3, 3))
    thread4 = threading.Thread(target=__crawling_pages__, args=(pages // 6 * 3, pages // 6 * 4, news_container4, 4))
    thread5 = threading.Thread(target=__crawling_pages__, args=(pages // 6 * 4, pages // 6 * 5, news_container5, 5))
    thread6 = threading.Thread(target=__crawling_pages__, args=(pages // 6 * 5, pages, news_container6, 6))

    thread1.start()
    thread2.start()
    thread3.start()
    thread4.start()
    thread5.start()
    thread6.start()

    thread1.join()
    thread2.join()
    thread3.join()
    thread4.join()
    thread5.join()
    thread6.join()


    try:
        news_container1.to_excel("./news1.xlsx")
    except:
        print("Ошибка в потоке 1!")
    try:
        news_container2.to_excel("./news2.xlsx")
    except:
        print("Ошибка в потоке 2!")
    try:
        news_container3.to_excel("./news3.xlsx")
    except:
        print("Ошибка в потоке 3!")
    try:
        news_container4.to_excel("./news4.xlsx")
    except:
        print("Ошибка в потоке 4!")
    try:
        news_container5.to_excel("./news5.xlsx")
    except:
        print("Ошибка в потоке 5!")
    try:
        news_container6.to_excel("./news6.xlsx")
    except:
        print("Ошибка в потоке 6!")


    try:
        news = pd.concat([news_container1, news_container2, news_container3, news_container4, news_container5, news_container6], ignore_index=True)
        news.to_excel("./news.xlsx")
    except:
        print("Не получилось!")


    if off_pc:
        __shutdown__()


crawling_pages(False, 1738)
