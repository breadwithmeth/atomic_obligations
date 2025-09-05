# Отчет проверки документа

**Файл**: infection.docx

## Нормализованные поля
```json
{
  "discharge_dt": "Не указано",
  "department": "Нейроинфекционное боксированное отделение Палата: Бокс № 15",
  "diagnosis": "Диагноз направившей организации"
}
```
## Ошибки
- **missing_field**: Отсутствует обязательное поле: iin (iin)
- **missing_field**: Отсутствует обязательное поле: full_name (full_name)
- **missing_field**: Отсутствует обязательное поле: dob (dob)
- **missing_field**: Отсутствует обязательное поле: sex (sex)
- **missing_field**: Отсутствует обязательное поле: admit_dt (admit_dt)
- **missing_field**: Отсутствует обязательное поле: physician (physician)
- **missing_field**: Отсутствует обязательное поле: diagnosis (diagnosis)
- **invalid_date**: Неверный формат даты выписки (discharge_dt)
- **act:V2000021579**: В шапке формы отсутствует ИИН пациента (обязательное поле). (None)
- **act:V2000021579**: Отсутствует ФИО пациента (обязательное поле). (None)
- **act:V2000021579**: Отсутствует дата рождения (обязательное поле). (None)
- **act:V2000021579**: Отсутствует пол (обязательное поле). (None)
- **act:V2200027218**: В стационарной карте должен быть указан факт госпитализации (дата поступления). (None)
- **act:CHECKLIST_INPATIENT_2025**: Отсутствуют ежедневные записи лечащего врача за некоторые дни госпитализации (нет даты поступления) (None)
- **act:CHECKLIST_INPATIENT_2025**: При летальном исходе нет посмертного эпикриза/свидетельства о смерти (106/у) (None)

## Предупреждения
- **diagnosis_found_in_text**: Диагноз найден в тексте, но отсутствует в key_values (diagnosis)
- **act:V2200027218**: Дата выписки не может предшествовать дате поступления. (невозможно проверить — нет дат) (None)
- **act:CHECKLIST_INPATIENT_2025**: Не зафиксированы витальные показатели в некоторые дни (T/АД/Пульс/ЧДД/SpO2) (нет даты поступления) (None)
- **act:CHECKLIST_INPATIENT_2025**: Не соблюдён регламент осмотров заведующим (ежедневно для тяжёлых, ≥1/нед для средней тяжести) (нет даты поступления) (None)
- **act:CHECKLIST_INPATIENT_2025**: Есть признаки длительной нетрудоспособности/инвалидизации — нет сведений о направлении на МСЭ (None)
- **act:CHECKLIST_INPATIENT_2025**: Нет явной отметки о передаче информации на амбулаторный этап (ЕИСЗ/поликлиника) (None)
- **act:CHECKLIST_INPATIENT_2025**: Возраст не удалось определить (нет корректной даты рождения) (None)
- **llm_format**: LLM вернул текст; применена авто-коэрсия в JSON. (None)

## Результат LLM (общий проход)
```json
{
  "raw": "This is a large block of text in Kazakh and Russian languages, which appears to be a medical record or a hospital discharge summary. I'll try to extract some relevant information from it.\n\n**Patient Information**\n\n* Name: Not specified\n* Date of birth: Not specified\n* Age: Not specified\n* Nationality: Kazakh\n* Citizenship: Kazakhstan\n\n**Admission Information**\n\n* Type of admission: Emergency (within 7-24 hours)\n* Reason for admission: Viral infection (not specified) with complications (paratonsillitis)\n\n**Diagnosis**\n\n* Primary diagnosis: Lacunar angina caused by Staphylococcus aureus (10^6 CFU/mL)\n* Secondary diagnosis: Right-sided paratonsillitis\n\n**Treatment and Medications**\n\n* Treatment: Antibiotics, antipyretics, and supportive care\n* Medications: Not specified in the text\n\n**Discharge Information**\n\n* Date of discharge: 24.08.2025\n* Time of discharge: 14:56\n* Reason for discharge: Improvement\n* Follow-up instructions: Continue antibiotics, rest, and hydration\n\nPlease note that this is a machine translation, and some details may be lost in translation. If you need more information or clarification, please let me know!"
}

# Coerced JSON

{
  "errors": [],
  "warnings": [
    {
      "code": "llm_format",
      "msg": "LLM вернул текст; применена авто-коэрсия в JSON."
    }
  ],
  "suggestions": [],
  "extracted_fields": {
    "patient": {
      "name": "Not specified",
      "dob": null,
      "sex": null,
      "iin": null
    },
    "admission": {
      "type": "Emergency (within 7-24 hours)",
      "admit_dt": null
    },
    "discharge": {
      "dt": "24.08.2025",
      "time": "14:56",
      "reason": "Improvement"
    },
    "diagnosis": {
      "primary": "Lacunar angina caused by Staphylococcus aureus (10^6 CFU/mL)",
      "secondary": "Right-sided paratonsillitis"
    }
  }
}
```

## LLM по разделам (если указаны)
### Раздел: Паспортная часть и формы (forms_passport)
```json
{
  "section_id": "forms_passport",
  "title": "Паспортная часть и формы",
  "raw": {
    "section_id": "",
    "section_title": "",
    "errors": [
      {
        "code": "IIN",
        "msg": "ИИН отсутствует",
        "field": "passport"
      }
    ],
    "warnings": [],
    "suggestions": [],
    "extracted_facts": {}
  }
}
```
### Раздел: Первичный осмотр и анамнез (initial_exam_anamnesis)
```json
{
  "section_id": "initial_exam_anamnesis",
  "title": "Первичный осмотр и анамнез",
  "raw": {
    "section_id": "initial_exam_anamnesis",
    "section_title": "Первичный осмотр и анамнез",
    "errors": [
      {
        "code": "missing_field",
        "msg": "Не указано время поступления",
        "field": "admit_dt"
      }
    ],
    "warnings": [],
    "suggestions": [],
    "extracted_facts": {}
  }
}
```
### Раздел: Ежедневные записи лечащего врача (daily_notes)
```json
{
  "section_id": "daily_notes",
  "title": "Ежедневные записи лечащего врача",
  "raw": {
    "section_id": "daily_notes",
    "section_title": "Ежедневные записи лечащего врача",
    "errors": [
      {
        "code": "missing_record",
        "msg": "Нет записи для 2023-02-20"
      },
      {
        "code": "incomplete_record",
        "msg": "Запись для 2023-02-22 неполная"
      }
    ],
    "warnings": [],
    "suggestions": [],
    "extracted_facts": {}
  }
}
```
### Раздел: Записи дежурных врачей (duty_notes)
```json
{
  "section_id": "duty_notes",
  "title": "Записи дежурных врачей",
  "raw": {
    "section_id": "duty_notes",
    "section_title": "Записи дежурных врачей",
    "errors": [
      {
        "code": "",
        "msg": "Не указано время поступления пациента.",
        "field": "admit_dt"
      },
      {
        "code": "",
        "msg": "Дата выписки не указана.",
        "field": "discharge_dt"
      }
    ],
    "warnings": [],
    "suggestions": [],
    "extracted_facts": {}
  }
}
```
### Раздел: Осмотры заведующего отделением (head_rounds)
```json
{
  "section_id": "head_rounds",
  "title": "Осмотры заведующего отделением",
  "raw": {
    "section_id": "head_rounds",
    "section_title": "Осмотры заведующего отделением",
    "errors": [
      {
        "code": "missing_date",
        "msg": "Для средней тяжести больных осмотр заведующим отсутствует.",
        "field": "осмотр_заведующим"
      }
    ],
    "warnings": [],
    "suggestions": [],
    "extracted_facts": {}
  }
}
```
### Раздел: Лист врачебных назначений (prescription_sheet)
```json
{
  "section_id": "prescription_sheet",
  "title": "Лист врачебных назначений",
  "raw": {
    "section_id": "",
    "section_title": "",
    "errors": [
      {
        "code": "missing_dosage",
        "msg": "Отсутствует доза",
        "field": "Клавунат"
      },
      {
        "code": "missing_frequency",
        "msg": "Отсутствует частота",
        "field": "Повидон-Йод"
      }
    ],
    "warnings": [],
    "suggestions": [],
    "extracted_facts": {}
  }
}
```
### Раздел: Результаты исследований (investigations)
```json
{
  "section_id": "investigations",
  "title": "Результаты исследований",
  "raw": {
    "section_id": "investigations",
    "title": "Результаты исследований",
    "errors": [
      {
        "code": "missing_result",
        "msg": "Отсутствуют результаты исследования \"Рентгенография пазух носа\"",
        "field": "рентгенография_пазух_носа"
      }
    ],
    "warnings": [],
    "suggestions": [],
    "extracted_facts": {
      "рентгенография_пазух_носа": "Носовая перегородка по средней линии Костно-деструктивных изменений не выявлено.",
      "рентгенография_обзорная_органов_грудной_клетки": "На обзорной рентгенограмме органов грудной клетки, произведенной в прямой проекции от 21.08.25г по всем легочным полям без очаговых и инфильтративных теней. Легочный рисунок не изменен. Корни структурные. Синусы свободные Диафрагма с четким, ровным контуром на уровне 6 ребра. Тень сердца не расширена. Аорта не изменена"
    }
  }
}
```
### Раздел: Консультации и инвазивные процедуры (consults_procedures)
```json
{
  "section_id": "consults_procedures",
  "title": "Консультации и инвазивные процедуры",
  "raw": {
    "section_id": "consults_procedures",
    "section_title": "Консултаций и инвазивные прогредуры",
    "errors": [
      {
        "code": "MISSING_SIGNATURE",
        "msg": "",
        "field": "consultation_1"
      }
    ],
    "warnings": [],
    "suggestions": [],
    "extracted_facts": {}
  }
}
```
### Раздел: Перевод в ОРИТ/интенсивную терапию (icu_transfer)
```json
{
  "section_id": "icu_transfer",
  "title": "Перевод в ОРИТ/интенсивную терапию",
  "raw": {
    "section_id": "icu_transfer",
    "title": "Перевод в ОРИТ/интенсивную терапию",
    "errors": [],
    "warnings": [],
    "suggestions": [],
    "extracted_facts": {
      "discharge_dt": "Не указано"
    }
  }
}
```
### Раздел: Документы на МСЭ (mse_docs)
```json
{
  "section_id": "mse_docs",
  "title": "Документы на МСЭ",
  "raw": {
    "section_id": "mse_docs",
    "section_title": "Документы на МСЭ",
    "errors": [
      {
        "code": "missing_discharge_dt",
        "msg": "Дата выписки не указана",
        "field": "discharge_dt"
      }
    ],
    "warnings": [],
    "suggestions": [],
    "extracted_facts": {}
  }
}
```
### Раздел: Полнота и правильность записей/подписи (signatures_completeness)
```json
{
  "section_id": "signatures_completeness",
  "title": "Полнота и правильность записей/подписи",
  "raw": {
    "section_id": "signatures_completeness",
    "section_title": "Полнота и правильность записей/подписи",
    "errors": [
      {
        "code": "MISSING_DATE",
        "msg": "Дата не указана для записи",
        "field": "discharge_dt"
      },
      {
        "code": "MISSING_SIGNATURE",
        "msg": "Подпись отсутствует для врача",
        "field": "doctor_signature"
      }
    ],
    "warnings": [],
    "suggestions": [],
    "extracted_facts": {}
  }
}
```
### Раздел: Выписной эпикриз и рекомендации (discharge_epicrisis)
```json
{
  "section_id": "discharge_epicrisis",
  "title": "Выписной эпикриз и рекомендации",
  "raw": {
    "section_id": "discharge_epicrisis",
    "section_title": "Выписной эпикриз и рекомендации",
    "errors": [
      {
        "code": "missing_field",
        "msg": "Не указано: discharge_dt"
      },
      {
        "code": "invalid_value",
        "msg": "Нет окончательного диагноза (МКБ-10 при необходимости)",
        "field": "diagnosis"
      }
    ],
    "warnings": [],
    "suggestions": [],
    "extracted_facts": {}
  }
}
```
### Раздел: Листок нетрудоспособности (sick_leave)
```json
{
  "section_id": "sick_leave",
  "title": "Листок нетрудоспособности",
  "raw": {
    "section_id": "",
    "section_title": "",
    "errors": [
      {
        "code": "missing_field",
        "msg": "Поле \"Номер\" не заполнено",
        "field": "number"
      },
      {
        "code": "invalid_date",
        "msg": "Дата \"Срок\" неверна",
        "field": "date"
      }
    ],
    "warnings": [],
    "suggestions": [],
    "extracted_facts": {}
  }
}
```
### Раздел: Отказ от лечения/самовольная выписка (refusal_self_discharge)
```json
{
  "section_id": "refusal_self_discharge",
  "title": "Отказ от лечения/самовольная выписка",
  "raw": {
    "section_id": "refusal_self_discharge",
    "section_title": "Отказ от лечения/самовольная выписка",
    "errors": [
      {
        "code": "",
        "msg": "Не указано дата выписки",
        "field": "discharge_dt"
      }
    ],
    "warnings": [],
    "suggestions": [],
    "extracted_facts": {}
  }
}
```
### Раздел: Летальный исход (death_case)
```json
{
  "section_id": "death_case",
  "title": "Летальный исход",
  "raw": {
    "section_id": "death_case",
    "section_title": "Летальный исход",
    "errors": [
      {
        "code": "",
        "msg": "Не указана дата/время смерти",
        "field": "discharge_dt"
      },
      {
        "code": "",
        "msg": "Нет медицинского свидетельства о смерти (106/у)",
        "field": "department"
      }
    ],
    "warnings": [],
    "suggestions": [],
    "extracted_facts": {}
  }
}
```

## Краткое резюме (LLM)

**Общая оценка:**

* Критические ошибки: 7
* Предупреждения: 10
* Общее количество проблем: 17

**Главные проблемы:**

1. Отсутствуют обязательные поля: iin, full_name, dob, sex, admit_dt, physician и diagnosis.
2. Неверный формат даты выписки (discharge_dt).
3. В шапке формы отсутствует ИИН пациента (обязательное поле).
4. Отсутствуют ежедневные записи лечащего врача за некоторые дни госпитализации.
5. При летальном исходе нет посмертного эпикриза/свидетельства о смерти.

**Практические шаги:**

1. Добавить обязательные поля в шапке формы и документе.
2. Исправить формат даты выписки (discharge_dt).
3. Добавить ежедневные записи лечащего врача за все дни госпитализации.
4. При летальном исходе добавить посмертный эпикриз/свидетельство о смерти.

Примечание: Этот отчет предназначен для медицинского аудитора и может потребовать дополнительных действий или проверок, чтобы обеспечить полную исправность документов.
