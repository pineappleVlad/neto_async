import asyncio
import aiohttp
from more_itertools import chunked
from sqlalchemy import select, delete

from models import Base, Session, SwapiPeople, engine

MAX_REQUESTS_CHUNK = 5


async def insert_people(people_list_json):
    people_list = []
    for person in people_list_json:
        birth_year = person.get('birth_year', None)
        eye_color = person.get('eye_color', None)
        films = ' , '.join(person.get('films', []))
        gender = person.get('gender', None)
        hair_color = person.get('hair_color', None)
        height = person.get('height', None)
        homeworld = person.get('homeworld', None)
        mass = person.get('mass', None)
        name = person.get('name', None)
        skin_color = person.get('skin_color', None)
        species = ' , '.join(person.get('species', []))
        starships = ' , '.join(person.get('starships', []))
        vehicles = ' , '.join(person.get('vehicles', []))

        swapi_person = SwapiPeople(
            birth_year=birth_year,
            eye_color=eye_color,
            films=films,
            gender=gender,
            hair_color=hair_color,
            height=height,
            homeworld=homeworld,
            mass=mass,
            name=name,
            skin_color=skin_color,
            species=species,
            starships=starships,
            vehicles=vehicles
        )

        people_list.append(swapi_person)
    async with Session() as session:
        session.add_all(people_list)
        await session.commit()


async def get_people(people_id):
    session = aiohttp.ClientSession()
    response = await session.get(f"https://swapi.dev/api/people/{people_id}")
    json_data = await response.json()
    await session.close()
    return json_data


async def main():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await clear_table()

    for person_ids_chunk in chunked(range(1, 80), MAX_REQUESTS_CHUNK):
        person_coros = [get_people(person_id) for person_id in person_ids_chunk]
        people = await asyncio.gather(*person_coros)
        insert_people_coro = insert_people(people)
        asyncio.create_task(insert_people_coro)

    main_task = asyncio.current_task()
    insets_tasks = asyncio.all_tasks() - {main_task}
    await asyncio.gather(*insets_tasks)

async def clear_table():
    async with Session() as session:
        async with session.begin():
            delete_statement = delete(SwapiPeople)
            await session.execute(delete_statement)
    print("Таблица SwapiPeople была успешно очищена.")

if __name__ == "__main__":
    asyncio.run(main())
