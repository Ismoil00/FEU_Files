select accounting.temp_estimates(
	'
		[
  {
    "name": {
      "ru": "Расходы",
      "tj": null
    },
    "estimate": 2,
    "parent_id": null,
    "isEstimate": false
  },
  {
    "name": {
      "ru": "Оплата труда работников",
      "tj": null
    },
    "estimate": 21,
    "parent_id": 2,
    "isEstimate": false
  },
  {
    "name": {
      "ru": "Оплата труда работников и налоговые отчисления",
      "tj": null
    },
    "estimate": 211,
    "parent_id": 21,
    "isEstimate": false
  },
  {
    "name": {
      "ru": "Заработная плата",
      "tj": null
    },
    "estimate": 2111,
    "parent_id": 211,
    "isEstimate": true
  },
  {
    "name": {
      "ru": "Налоговые отчисления",
      "tj": null
    },
    "estimate": 212,
    "parent_id": 21,
    "isEstimate": false
  },
  {
    "name": {
      "ru": "Взносы отчисления на социальные нужды",
      "tj": null
    },
    "estimate": 2121,
    "parent_id": 212,
    "isEstimate": true
  },
  {
    "name": {
      "ru": "Расходы на товары и услуги",
      "tj": null
    },
    "estimate": 22,
    "parent_id": 2,
    "isEstimate": false
  },
  {
    "name": {
      "ru": "Приобретение товаров и услуг",
      "tj": null
    },
    "estimate": 221,
    "parent_id": 22,
    "isEstimate": false
  },
  {
    "name": {
      "ru": "Товарно-материальные запасы",
      "tj": null
    },
    "estimate": 2211,
    "parent_id": 221,
    "isEstimate": true
  },
  {
    "name": {
      "ru": "Приобретение технических товаров",
      "tj": null
    },
    "estimate": 2212,
    "parent_id": 221,
    "isEstimate": true
  },
  {
    "name": {
      "ru": "Горюче-смазочные материалы",
      "tj": null
    },
    "estimate": 2213,
    "parent_id": 221,
    "isEstimate": true
  },
  {
    "name": {
      "ru": "Текущие расходы, кроме ремонта",
      "tj": null
    },
    "estimate": 2214,
    "parent_id": 221,
    "isEstimate": true
  },
  {
    "name": {
      "ru": "Текущий ремонт",
      "tj": null
    },
    "estimate": 2215,
    "parent_id": 221,
    "isEstimate": true
  },
  {
    "name": {
      "ru": "Оплата услуг специалистов",
      "tj": null
    },
    "estimate": 2216,
    "parent_id": 221,
    "isEstimate": true
  },
  {
    "name": {
      "ru": "Оплата за коммунальные услуги",
      "tj": null
    },
    "estimate": 2217,
    "parent_id": 221,
    "isEstimate": true
  },
  {
    "name": {
      "ru": "Оплата за услуги связи",
      "tj": null
    },
    "estimate": 2218,
    "parent_id": 221,
    "isEstimate": true
  },
  {
    "name": {
      "ru": "Товары и услуги, не отнесенные к другим категориям",
      "tj": null
    },
    "estimate": 2219,
    "parent_id": 221,
    "isEstimate": true
  },
  {
    "name": {
      "ru": "Расходы на выплату процентов",
      "tj": null
    },
    "estimate": 23,
    "parent_id": 2,
    "isEstimate": false
  },
  {
    "name": {
      "ru": "Проценты",
      "tj": null
    },
    "estimate": 231,
    "parent_id": 23,
    "isEstimate": false
  },
  {
    "name": {
      "ru": "Проценты нерезидентам",
      "tj": null
    },
    "estimate": 2311,
    "parent_id": 231,
    "isEstimate": true
  },
  {
    "name": {
      "ru": "Проценты резидентам, кроме сектора государственного управления",
      "tj": null
    },
    "estimate": 2312,
    "parent_id": 231,
    "isEstimate": true
  },
  {
    "name": {
      "ru": "Проценты единицам сектора государственного управления",
      "tj": null
    },
    "estimate": 2313,
    "parent_id": 231,
    "isEstimate": true
  },
  {
    "name": {
      "ru": "Расходы на выплаты субсидий",
      "tj": null
    },
    "estimate": 24,
    "parent_id": 2,
    "isEstimate": false
  },
  {
    "name": {
      "ru": "Государственным корпорациям (организациям)",
      "tj": null
    },
    "estimate": 241,
    "parent_id": 24,
    "isEstimate": false
  },
  {
    "name": {
      "ru": "Финансовые и нефинансовые государственные корпорации (организациям)",
      "tj": null
    },
    "estimate": 2411,
    "parent_id": 241,
    "isEstimate": true
  },
  {
    "name": {
      "ru": "Частным корпорациям (организациям)",
      "tj": null
    },
    "estimate": 2412,
    "parent_id": 241,
    "isEstimate": true
  },
  {
    "name": {
      "ru": "Расходы на выделение грантов",
      "tj": null
    },
    "estimate": 25,
    "parent_id": 2,
    "isEstimate": false
  },
  {
    "name": {
      "ru": "Гранты",
      "tj": null
    },
    "estimate": 251,
    "parent_id": 25,
    "isEstimate": false
  },
  {
    "name": {
      "ru": "Иностранным Правительствам",
      "tj": null
    },
    "estimate": 2511,
    "parent_id": 251,
    "isEstimate": true
  },
  {
    "name": {
      "ru": "Международным организациям (текущие)",
      "tj": null
    },
    "estimate": 2512,
    "parent_id": 251,
    "isEstimate": true
  },
  {
    "name": {
      "ru": "Международным организациям (капитальные)",
      "tj": null
    },
    "estimate": 2513,
    "parent_id": 251,
    "isEstimate": true
  },
  {
    "name": {
      "ru": "Другим единицам сектора государственного управления (текущие)",
      "tj": null
    },
    "estimate": 2514,
    "parent_id": 251,
    "isEstimate": true
  },
  {
    "name": {
      "ru": "Другим единицам сектора государственного управления (капитальные)",
      "tj": null
    },
    "estimate": 2515,
    "parent_id": 251,
    "isEstimate": true
  },
  {
    "name": {
      "ru": "Расходы на пособия социального обеспечение и помощи",
      "tj": null
    },
    "estimate": 26,
    "parent_id": 2,
    "isEstimate": false
  },
  {
    "name": {
      "ru": "Пособия",
      "tj": null
    },
    "estimate": 261,
    "parent_id": 26,
    "isEstimate": false
  },
  {
    "name": {
      "ru": "Пособия по социальному обеспечению",
      "tj": null
    },
    "estimate": 2611,
    "parent_id": 261,
    "isEstimate": true
  },
  {
    "name": {
      "ru": "Пособия по социальной помощи",
      "tj": null
    },
    "estimate": 2612,
    "parent_id": 261,
    "isEstimate": true
  },
  {
    "name": {
      "ru": "Другие расходы",
      "tj": null
    },
    "estimate": 27,
    "parent_id": 2,
    "isEstimate": false
  },
  {
    "name": {
      "ru": "Расходы, связанные с собственностью, кроме процентов",
      "tj": null
    },
    "estimate": 271,
    "parent_id": 27,
    "isEstimate": false
  },
  {
    "name": {
      "ru": "Расходы, связанные с собственностью, вмененные держателям страховых полисов ",
      "tj": null
    },
    "estimate": 2711,
    "parent_id": 271,
    "isEstimate": true
  },
  {
    "name": {
      "ru": "Расходы на ренту",
      "tj": null
    },
    "estimate": 2712,
    "parent_id": 271,
    "isEstimate": true
  },
  {
    "name": {
      "ru": "Расходы на страхование зданий и оборудования ",
      "tj": null
    },
    "estimate": 2713,
    "parent_id": 271,
    "isEstimate": true
  },
  {
    "name": {
      "ru": "Другие расходы, не отнесенные к другим подразделам 27100.",
      "tj": null
    },
    "estimate": 2714,
    "parent_id": 271,
    "isEstimate": true
  },
  {
    "name": {
      "ru": "Различные прочие расходы",
      "tj": null
    },
    "estimate": 272,
    "parent_id": 27,
    "isEstimate": false
  },
  {
    "name": {
      "ru": "Стипендии",
      "tj": null
    },
    "estimate": 2721,
    "parent_id": 272,
    "isEstimate": true
  },
  {
    "name": {
      "ru": "Пенсии",
      "tj": null
    },
    "estimate": 2722,
    "parent_id": 272,
    "isEstimate": true
  },
  {
    "name": {
      "ru": "Компенсации на продукты питания",
      "tj": null
    },
    "estimate": 2723,
    "parent_id": 272,
    "isEstimate": true
  },
  {
    "name": {
      "ru": "Компенсация на другие товары",
      "tj": null
    },
    "estimate": 2724,
    "parent_id": 272,
    "isEstimate": true
  },
  {
    "name": {
      "ru": "Оплата штрафов и пени",
      "tj": null
    },
    "estimate": 2725,
    "parent_id": 272,
    "isEstimate": true
  },
  {
    "name": {
      "ru": "Трансферты некоммерческим организациям",
      "tj": null
    },
    "estimate": 2726,
    "parent_id": 272,
    "isEstimate": true
  },
  {
    "name": {
      "ru": "Единовременная компенсация",
      "tj": null
    },
    "estimate": 2727,
    "parent_id": 272,
    "isEstimate": true
  },
  {
    "name": {
      "ru": "Трансферты, не отнесенные к другим категориям",
      "tj": null
    },
    "estimate": 2728,
    "parent_id": 272,
    "isEstimate": true
  },
  {
    "name": {
      "ru": "Расходы на капитальные цели",
      "tj": null
    },
    "estimate": 273,
    "parent_id": 27,
    "isEstimate": false
  },
  {
    "name": {
      "ru": "Внутренние капитальные трансферты нефинансовым государственным предприятиям",
      "tj": null
    },
    "estimate": 2731,
    "parent_id": 273,
    "isEstimate": true
  },
  {
    "name": {
      "ru": "Внутренние капитальные трансферты государственным финансовым учреждениям",
      "tj": null
    },
    "estimate": 2732,
    "parent_id": 273,
    "isEstimate": true
  },
  {
    "name": {
      "ru": "Внутренние капитальные трансферты нефинансовым частным предприятиям",
      "tj": null
    },
    "estimate": 2733,
    "parent_id": 273,
    "isEstimate": true
  },
  {
    "name": {
      "ru": "Внутренние капитальные трансферты частным финансовым предприятиям",
      "tj": null
    },
    "estimate": 2734,
    "parent_id": 273,
    "isEstimate": true
  },
  {
    "name": {
      "ru": "Внутренние капитальные трансферты, не отнесенные к другим категориям",
      "tj": null
    },
    "estimate": 2735,
    "parent_id": 273,
    "isEstimate": true
  },
  {
    "name": {
      "ru": "Операции с активами и обязательствами",
      "tj": null
    },
    "estimate": 28,
    "parent_id": 2,
    "isEstimate": false
  },
  {
    "name": {
      "ru": "Операции с нефинансовыми активами",
      "tj": null
    },
    "estimate": 281,
    "parent_id": 28,
    "isEstimate": false
  },
  {
    "name": {
      "ru": "Здания и сооружения (жилые помещения)",
      "tj": null
    },
    "estimate": 2811,
    "parent_id": 281,
    "isEstimate": true
  },
  {
    "name": {
      "ru": "Здания и сооружения (нежилые помещения)",
      "tj": null
    },
    "estimate": 2812,
    "parent_id": 281,
    "isEstimate": true
  },
  {
    "name": {
      "ru": "Здания и сооружения (прочие сооружения)",
      "tj": null
    },
    "estimate": 2813,
    "parent_id": 281,
    "isEstimate": true
  },
  {
    "name": {
      "ru": "Машины и оборудование",
      "tj": null
    },
    "estimate": 2814,
    "parent_id": 281,
    "isEstimate": true
  },
  {
    "name": {
      "ru": "Нематериальные основные фонды",
      "tj": null
    },
    "estimate": 2815,
    "parent_id": 281,
    "isEstimate": true
  },
  {
    "name": {
      "ru": "Запасы материальных оборотных средств",
      "tj": null
    },
    "estimate": 2816,
    "parent_id": 281,
    "isEstimate": true
  },
  {
    "name": {
      "ru": "Непроизводственные активы",
      "tj": null
    },
    "estimate": 2817,
    "parent_id": 281,
    "isEstimate": true
  },
  {
    "name": {
      "ru": "Приобретение внутренних финансовых активов",
      "tj": null
    },
    "estimate": 282,
    "parent_id": 28,
    "isEstimate": false
  },
  {
    "name": {
      "ru": "Увеличение остатков по валюте и депозитам",
      "tj": null
    },
    "estimate": 2821,
    "parent_id": 282,
    "isEstimate": true
  },
  {
    "name": {
      "ru": "Кредитование и приобретение акций",
      "tj": null
    },
    "estimate": 2822,
    "parent_id": 282,
    "isEstimate": true
  },
  {
    "name": {
      "ru": "Приобретение внешних финансовых активов",
      "tj": null
    },
    "estimate": 283,
    "parent_id": 28,
    "isEstimate": false
  },
  {
    "name": {
      "ru": "Увеличение остатков по валюте и депозитам",
      "tj": null
    },
    "estimate": 2831,
    "parent_id": 283,
    "isEstimate": true
  },
  {
    "name": {
      "ru": "Внешнее кредитование",
      "tj": null
    },
    "estimate": 2832,
    "parent_id": 283,
    "isEstimate": true
  },
  {
    "name": {
      "ru": "Погашение внутренних финансовых обязательств",
      "tj": null
    },
    "estimate": 284,
    "parent_id": 28,
    "isEstimate": false
  },
  {
    "name": {
      "ru": "Погашение ценных бумаг, кроме акций",
      "tj": null
    },
    "estimate": 2841,
    "parent_id": 284,
    "isEstimate": true
  },
  {
    "name": {
      "ru": "Выплаты кредитов и ссуд",
      "tj": null
    },
    "estimate": 2842,
    "parent_id": 284,
    "isEstimate": true
  },
  {
    "name": {
      "ru": "Погашение внешних финансовых обязательств",
      "tj": null
    },
    "estimate": 2843,
    "parent_id": 284,
    "isEstimate": true
  },
  {
    "name": {
      "ru": "Медицинское страхование и другие виды страхования",
      "tj": "Сугуртаи тибби ва дигар сугуртахо"
    },
    "estimate": 2122,
    "parent_id": null,
    "isEstimate": true
  },
  {
    "name": {
      "ru": "Выплата заработной платы техническому персоналу",
      "tj": "Пардохти музди мехнати кормандони хайати техники"
    },
    "estimate": 2221,
    "parent_id": null,
    "isEstimate": true
  },
  {
    "name": {
      "ru": "Выплаты / ассигнования на социальные нужды",
      "tj": "Пардохтхо/маблагчудокунихо ба эхтиёчоти ичтимои"
    },
    "estimate": 2222,
    "parent_id": null,
    "isEstimate": true
  }
]
	'
)








select * from commons.accounts_chapter

select * from commons.additional_payment

select * from commons.budget_distribution_types

select * from commons.countries

select * from commons.deceased_staff_groups

select * from commons.department

select * from commons.disabled_staff_groups

select * from commons.disciplinary_action_type

select * from commons.dismissal_type

select * from commons.document_type

select * from commons.financial_aids

select * from commons.gender

select * from commons.global_units

select * from commons.jobtitle

select * from commons.local_trip_prices;

select * from commons.marker;

select * from commons.militaryrank

select * from commons.pension_awards

select * from commons.pension_percentage_for_25_years

select * from commons.pension_restrictions

select * from commons.pension_type

select * from commons.rank_group

select * from commons.region

select * from commons.retention

select * from commons.salary_addition_type

select * from commons.social_tax

select * from commons.staff_status

select * from commons.trip_daily_pay

select * from commons.vacation_group


select * from payments.base_indicator

select * from payments.payment_purpose






create or replace function auth.temp_module (jdata jsonb)
returns void
language plpgsql
as $$
declare
  _row jsonb;
begin

  for _row in select jsonb_array_elements(jdata) loop

    insert into accounting.estimates (
    estimate,
	name,
	parent_id,
	"isEstimate"
    ) values (
      (_row->>'estimate')::int,
      (_row->>'name')::jsonb,
	  (_row->>'parent_id')::int,
      (_row->>'isEstimate')::bool
    );

  end loop;
end;
$$;




----------------------------------------------------------------
----------------------------------------------------------------


select * from commons.nomenclature
where product_category_id in (13, 14)


select * from commons.nomenclature
where product_category_id not in (13, 14)

select * from commons.global_units

select * from commons.product_category



Штук 2
комплект 26
пара 27
упаковка 30
Килограмм 6
Метр 1
Рулон 28



73


INSERT INTO commons.nomenclature
    (name, code, product_category_id, created, unit_id, properties, disabled)
VALUES
  ('Аптечка первая помощь', 'MD-001', '17', now(), 1, 'first aid kit basic', false),
  ('Расчёсыватель переломов', 'MD-002', '17', now(), 1, 'splint universal', false),
  ('Жгут турникет', 'MD-003', '17', now(), 1, 'tourniquet emergency', false),
  ('Носилки складные', 'MD-004', '17', now(), 1, 'folding stretcher', false),
  ('Дефибриллятор портативный', 'MD-005', '17', now(), 1, 'automated external', false),
  ('Маска кислородная', 'MD-006', '17', now(), 1, 'oxygen mask size M', false),
  ('Баллон кислородный 5л', 'MD-007', '17', now(), 3, '5L cylinder', false),
  ('Лекарства анальгетические', 'MD-008', '17', now(), 1, 'pain reliever tablets', false),
  ('Пакет перевязочный стерильный', 'MD-009', '17', now(), 1, 'sterile dressing 10x10', false),
  ('Раствор дезинфицирующий 5‑л', 'MD-010', '17', now(), 3, 'chlorhexidine solution', false),

  ('Окклюзионная повязка', 'MD-011', '17', now(), 1, 'occlusive sealing', false),
  ('Пинцет хирургический', 'MD-012', '17', now(), 1, 'tweezers stainless steel', false),
  ('Ножницы портативные медицинские', 'MD-013', '17', now(), 1, 'surgical scissors small', false),
  ('Шприц 5 мл стерильный', 'MD-014', '17', now(), 1, '5ml syringe', false),
  ('Перчатки медицинские латексные', 'MD-015', '17', now(), 5, 'latex gloves size L', false),
  ('Шина для конечностей', 'MD-016', '17', now(), 1, 'limb splint adjustable', false),
  ('Аптечка автомобильная', 'MD-017', '17', now(), 1, 'road emergency kit', false),
  ('Лампа хирургическая переносная', 'MD-018', '17', now(), 1, 'LED surgical lamp', false),
  ('Капельница комплект', 'MD-019', '17', now(), 2, 'IV drip set', false),
  ('Стерилизатор портативный', 'MD-020', '17', now(), 1, 'portable sterilizer', false),

  ('Антисептик спиртосодержащий', 'MD-021', '17', now(), 1, 'alcohol antiseptic', false),
  ('Тонометр цифровой', 'MD-022', '17', now(), 1, 'digital blood pressure', false),
  ('Стетоскоп классический', 'MD-023', '17', now(), 1, 'acoustic stethoscope', false),
  ('Термометр инфракрасный', 'MD-024', '17', now(), 1, 'infrared thermometer', false),
  ('Пульсоксиметр', 'MD-025', '17', now(), 1, 'pulse oximeter', false),
  ('Повязка компрессионная', 'MD-026', '17', now(), 1, 'compression bandage', false),
  ('Маска хирургическая', 'MD-027', '17', now(), 1, 'surgical mask 3‑ply', false),
  ('Очки защитные медицинские', 'MD-028', '17', now(), 1, 'safety goggles', false),
  ('Костюм защитный био', 'MD-029', '17', now(), 1, 'biohazard suit', false),
  ('Шприц 10 мл', 'MD-030', '17', now(), 1, '10ml syringe', false),

  ('Тубус для носилок', 'MD-031', '17', now(), 1, 'stretcher case', false),
  ('Дефибриллятор автоматический CAR', 'MD-032', '17', now(), 1, 'CAR AED type', false),
  ('Лекарства сердечные', 'MD-033', '17', now(), 1, 'cardio meds', false),
  ('Средства от ожогов', 'MD-034', '17', now(), 1, 'burn treatment', false),
  ('Перчатки нитриловые', 'MD-035', '17', now(), 5, 'nitrile gloves', false),
  ('Очистка ран раствор', 'MD-036', '17', now(), 1, 'wound cleaning solution', false),
  ('Труба дыхательная', 'MD-037', '17', now(), 1, 'breathing tube', false),
  ('Монитор пациента', 'MD-038', '17', now(), 1, 'patient monitor', false),
  ('Кровать медицинская', 'MD-039', '17', now(), 1, 'adjustable bed', false),
  ('Тележка медикаментозная', 'MD-040', '17', now(), 1, 'med cart mobile', false),

  ('Кислородная маска детская', 'MD-041', '17', now(), 1, 'oxygen mask child', false),
  ('Баллон кислородный 2л', 'MD-042', '17', now(), 3, '2L cylinder', false),
  ('Лекарства от аллергии', 'MD-043', '17', now(), 1, 'antihistamines', false),
  ('Седельник транспортный', 'MD-044', '17', now(), 1, 'transport seat', false),
  ('Крышка стерильная', 'MD-045', '17', now(), 1, 'sterile cover', false),
  ('Рукавички хирургические стерильные', 'MD-046', '17', now(), 5, 'surgical sterile gloves', false),
  ('Пилочка медицинская', 'MD-047', '17', now(), 1, 'nail file steel', false),
  ('Медицинский халат', 'MD-048', '17', now(), 1, 'doctor gown', false),
  ('Шапочка хирургическая', 'MD-049', '17', now(), 1, 'surgical cap', false),
  ('Очки защиты лицо', 'MD-050', '17', now(), 1, 'face shield', false),

  ('Дренаж медицинский', 'MD-051', '17', now(), 1, 'drain tube', false),
  ('Шина буккальная', 'MD-052', '17', now(), 1, 'oral splint', false),
  ('Аптечка ожоговая', 'MD-053', '17', now(), 1, 'burn first aid', false),
  ('Средство от заражений', 'MD-054', '17', now(), 1, 'infection control', false),
  ('Пакет для выделения органов', 'MD-055', '17', now(), 1, 'organ sample bag', false),
  ('Подушка медитативная', 'MD-056', '17', now(), 1, 'pressure relief pad', false),
  ('Устройство ИВЛ переносное', 'MD-057', '17', now(), 1, 'portable ventilator', false),
  ('Шприц инсулиновый', 'MD-058', '17', now(), 1, 'insulin syringe', false),
  ('Термометр ртутный', 'MD-059', '17', now(), 1, 'mercury thermometer', false),
  ('Калоприёмник одноразовый', 'MD-060', '17', now(), 1, 'disposable bedpan', false),

  ('Лекарства жаропонижающие', 'MD-061', '17', now(), 1, 'fever reducer', false),
  ('Средство от насморка', 'MD-062', '17', now(), 1, 'nasal spray', false),
  ('Капли глазные', 'MD-063', '17', now(), 1, 'eye drops', false),
  ('Таблетки от диареи', 'MD-064', '17', now(), 1, 'anti‑diarrheal', false),
  ('Пакет для рвоты', 'MD-065', '17', now(), 1, 'vomit bag', false),
  ('Шприц 2 мл', 'MD-066', '17', now(), 1, '2ml syringe', false),
  ('Промывалка ушная', 'MD-067', '17', now(), 1, 'ear irrigator', false),
  ('Медицинская ткань марля', 'MD-068', '17', now(), 7, 'gauze swab', false),
  ('Лекарства антисептические', 'MD-069', '17', now(), 1, 'antiseptics', false),
  ('Шапочка одноразовая', 'MD-070', '17', now(), 1, 'disposable cap', false),

  ('Пластырь бактерицидный', 'MD-071', '17', now(), 1, 'bandage plaster', false),
  ('Шприц 20 мл', 'MD-072', '17', now(), 1, '20ml syringe', false),
  ('Набор шовный', 'MD-073', '17', now(), 2, 'suture set', false),
  ('Дистресский набор', 'MD-074', '17', now(), 1, 'distress kit', false),
  ('Ороситель носовой', 'MD-075', '17', now(), 1, 'nasal irrigator', false),
  ('Стерилизатор паровой', 'MD-076', '17', now(), 1, 'steam sterilizer', false),
  ('Костыль подмышечный', 'MD-077', '17', now(), 1, 'axillary crutch', false),
  ('Колесо инвалидное', 'MD-078', '17', now(), 1, 'wheelchair', false),
  ('Тонометр механический', 'MD-079', '17', now(), 1, 'manual bp cuff', false),
  ('Маска анестезирующая', 'MD-080', '17', now(), 1, 'anesthetic mask', false),

  ('Лекарства спазмолитики', 'MD-081', '17', now(), 1, 'antispasmodics', false),
  ('Средство от укусов', 'MD-082', '17', now(), 1, 'bite treatment', false),
  ('Пакет хирургический стерильный', 'MD-083', '17', now(), 1, 'sterile pack large', false),
  ('Крышка стерилизация', 'MD-084', '17', now(), 1, 'sterilization cover', false),
  ('Шприц 50 мл', 'MD-085', '17', now(), 1, '50ml syringe', false),
  ('Набор перевязок', 'MD-086', '17', now(), 1, 'bandage set', false),
  ('Лекарства противорвотные', 'MD-087', '17', now(), 1, 'antiemetic', false),
  ('Средство для ожогов гель', 'MD-088', '17', now(), 1, 'burn gel', false),
  ('Пакет перевязочный большой', 'MD-089', '17', now(), 1, 'large dressing', false),
  ('Щипцы хирургические мелкие', 'MD-090', '17', now(), 1, 'small forceps', false),

  ('Защитный костюм химический', 'MD-091', '17', now(), 1, 'chemical protection', false),
  ('Лекарства противораковые (противоболевые)', 'MD-092', '17', now(), 1, 'oncology support', false),
  ('Маска респиратор FFP2', 'MD-093', '17', now(), 1, 'filter class FFP2', false),
  ('Очки защитные химические', 'MD-094', '17', now(), 1, 'chemical splash', false),
  ('Перчатки виниловые', 'MD-095', '17', now(), 5, 'vinyl gloves', false),
  ('Капсула кислородная', 'MD-096', '17', now(), 1, 'oxygen capsule', false),
  ('Средство от судорог', 'MD-097', '17', now(), 1, 'anticonvulsant', false),
  ('Пакет для крови', 'MD-098', '17', now(), 1, 'blood bag sterile', false),
  ('Шприц одноразовый 1 мл', 'MD-099', '17', now(), 1, '1ml syringe', false),
  ('Фонендоскоп взрослый', 'MD-100', '17', now(), 1, 'adult stethoscope', false);











