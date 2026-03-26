# Data Dictionary — Referral Report
### Springer Capital | Referral Program
**Audience:** Business Users (e.g. Marketing Managers, Operations Teams)
**Last Updated:** 2024
**Document Purpose:** Explains each column in the final referral report (`referral_report.csv`) in plain language.

---

## What Is This Report?

This report is generated automatically from the referral program's raw data.
Each row represents **one referral** — an instance where an existing member
(the *referrer*) invited someone new (the *referee*) to join the platform.

The final column, **`is_business_logic_valid`**, tells you at a glance whether
that referral is considered legitimate or potentially fraudulent based on
Springer Capital's business rules.

---

## Column Definitions

| # | Column Name | Plain English Name | Data Type | Example Value | Description |
|---|---|---|---|---|---|
| 1 | `referral_details_id` | Report Row Number | Whole Number | `101` | A unique sequential number assigned to each row in this report. Used for easy reference. |
| 2 | `referral_id` | Referral Code | Text | `9331c8f1…` | The unique system identifier for this referral event. |
| 3 | `referral_source` | How the Referral Was Made | Text | `User Sign Up` | The channel through which the referral was created. Possible values: **User Sign Up**, **Draft Transaction**, **Lead**. |
| 4 | `referral_source_category` | Referral Channel Category | Text | `Online` | A higher-level category for the referral channel. **Online** = User Sign Up. **Offline** = Draft Transaction. For Lead referrals, the category comes from the lead's own source record. |
| 5 | `referral_at` | Referral Date & Time | Date/Time | `2024-05-01 12:17:31` | The exact local date and time when the referral was created. Converted from UTC to the referrer's local timezone. |
| 6 | `referrer_id` | Referrer System ID | Text | `2c71c5d6…` | The system identifier of the member who made the referral (the person who invited someone). |
| 7 | `referrer_name` | Referrer Name | Text | `John Doe` | The name of the member who made the referral. |
| 8 | `referrer_phone_number` | Referrer Phone | Text | `abc123…` | The referrer's phone number (stored in hashed/encoded format for privacy). |
| 9 | `referrer_homeclub` | Referrer's Home Club | Text | `BENHIL` | The gym or club location the referrer is registered to. Club names are kept in their original format. |
| 10 | `referee_id` | Referee System ID | Text | `f1327c9d…` | The system identifier of the person who was referred (the new or prospective member). |
| 11 | `referee_name` | Referee Name | Text | `Jane Doe` | The name of the person who was referred. |
| 12 | `referee_phone` | Referee Phone | Text | `5ba638fe…` | The referee's phone number (stored in hashed/encoded format for privacy). |
| 13 | `referral_status` | Referral Outcome | Text | `Berhasil` | The current status of the referral. **Berhasil** = Successful. **Menunggu** = Pending. **Tidak Berhasil** = Unsuccessful. |
| 14 | `num_reward_days` | Reward Days Granted | Whole Number | `20` | The number of membership days awarded as a reward for this referral. Blank if no reward was assigned. |
| 15 | `transaction_id` | Transaction Reference | Text | `1d1eb8a9…` | The unique ID of the financial transaction linked to this referral. Blank if no transaction exists. |
| 16 | `transaction_status` | Payment Status | Text | `Paid` | The status of the linked transaction. **Paid** = payment completed successfully. |
| 17 | `transaction_at` | Transaction Date & Time | Date/Time | `2024-05-02 11:49:01` | The local date and time when the transaction was processed. |
| 18 | `transaction_location` | Transaction Location | Text | `ARTERI PONDOK INDAH` | The gym or club location where the transaction took place. |
| 19 | `transaction_type` | Transaction Category | Text | `New` | The type of transaction. **New** = a brand-new membership purchase. |
| 20 | `updated_at` | Last Updated | Date/Time | `2024-05-01 12:17:31` | The last time this referral record was updated in the system. |
| 21 | `reward_granted_at` | Reward Grant Date | Date/Time | `2024-06-02 20:42:09` | The date and time when the membership reward was actually granted to the referee. Blank if not yet granted. |
| 22 | `is_business_logic_valid` | Is This Referral Legitimate? | True / False | `True` | **The most important column.** `True` means the referral passes all business rules and is considered legitimate. `False` means one or more rules were violated — this referral may be fraudulent and should be reviewed. |

---

## Understanding `is_business_logic_valid`

### ✅ When is a referral marked TRUE (Legitimate)?

A referral is legitimate when **all** of the following are true:
- The reward value is greater than zero
- The referral status is **Berhasil** (Successful)
- A valid transaction is linked
- The transaction was **Paid**
- The transaction type is **New**
- The transaction happened **after** the referral was created
- The transaction occurred **in the same calendar month** as the referral
- The referrer's membership has **not expired**
- The referrer's account has **not been deleted**
- The reward has been **granted** to the referee

OR:

- The referral status is Pending or Unsuccessful **and** no reward was assigned (this is normal and expected).

---

### ❌ When is a referral marked FALSE (Potential Fraud)?

| Scenario | What It Means |
|---|---|
| Reward granted but status is not Successful | A reward was paid out for a referral that wasn't marked as successful — possible data error or manipulation |
| Reward granted but no transaction linked | A reward exists with no supporting purchase — suspicious |
| No reward but there is a paid transaction after the referral | A qualifying transaction exists but no reward was issued — possible missed reward or workaround attempt |
| Status is Successful but no reward assigned | The referral was marked successful but no reward was given — inconsistency |
| Transaction date is before referral date | The purchase happened before the referral was even created — logically impossible, indicates data tampering |

---

## Notes for Business Users

- **Privacy:** Phone numbers and some names appear as encoded text strings. This is intentional to protect member privacy. The data team can decode these for authorised investigations.
- **Timezones:** All dates and times in this report are in the **member's local time**, not UTC. This makes them easier to compare against receipts or support tickets.
- **Club Names:** Club location names (e.g. `BENHIL`, `PLUIT`) are kept in their original ALL-CAPS format as registered in the system.
- **Blank cells:** A blank value in `transaction_id`, `num_reward_days`, or `reward_granted_at` means that information does not exist for that referral — it is not an error.

---

*For technical questions about this report, contact the Data Engineering team.*
