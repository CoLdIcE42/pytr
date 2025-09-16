import csv
import json
import platform
from dataclasses import dataclass
from locale import getdefaultlocale
from typing import Any, Iterable, Literal, Optional, TextIO, TypedDict, Union

from babel.numbers import format_decimal

from .event import ConditionalEventType, Event, PPEventType
from .translation import setup_translation
from .utils import get_logger

SUPPORTED_LANGUAGES = {
    "cs",
    "da",
    "de",
    "en",
    "es",
    "fr",
    "it",
    "nl",
    "pl",
    "pt",
    "ru",
    "zh",
}

CSVCOLUMN_TO_TRANSLATION_KEY = {
    "date": "CSVColumn_Date",
    "type": "CSVColumn_Type",
    "value": "CSVColumn_Value",
    "note": "CSVColumn_Note",
    "isin": "CSVColumn_ISIN",
    "shares": "CSVColumn_Shares",
    "fees": "CSVColumn_Fees",
    "taxes": "CSVColumn_Taxes",
}


class _SimpleTransaction(TypedDict):
    date: str
    type: Union[str, None]
    value: Union[str, float, None]
    note: Union[str, float, None]
    isin: Union[str, float, None]
    shares: Union[str, float, None]
    fees: Union[str, float, None]
    taxes: Union[str, float, None]


@dataclass
class TransactionExporter:
    """
    A helper class to convert Trade Republic events each to one or more line items that are a simplified representation
    useful for a importing for example into a portfolio manager.
    """

    lang: str = "en"
    """ The language for the CSV header / JSON keys. """

    date_with_time: bool = True
    """ Include the timestamp in ISO8601 format in the date field. """

    decimal_localization: bool = False
    """ Whether to localize the decimal format. If enabled, decimal fields will be string values. """

    csv_delimiter: str = ";"

    def __post_init__(self):
        self._log = get_logger(__name__)

        if self.lang == "auto":
            locale = getdefaultlocale()[0]
            if locale is None:
                self.lang = "en"
            else:
                self.lang = locale.split("_")[0]

        if self.lang not in SUPPORTED_LANGUAGES:
            self._log.info(f'Language not yet supported "{self.lang}", defaulting to "en"')
            self.lang = "en"

        self._translate = setup_translation(language=self.lang)

    def _decimal_format(self, value: Optional[float], quantization: bool = True) -> Union[str, float, None]:
        if value is None:
            return None
        return (
            format_decimal(value, locale=self.lang, decimal_quantization=quantization)
            if self.decimal_localization
            else value
        )

    def _localize_keys(self, txn: _SimpleTransaction) -> dict[str, Any]:
        if self.lang is None:
            return
        return {self._translate(value): txn[key] for key, value in CSVCOLUMN_TO_TRANSLATION_KEY.items()}  # type: ignore[literal-required]

    def fields(self) -> list[str]:
        return [self._translate(value) for key, value in CSVCOLUMN_TO_TRANSLATION_KEY.items()]

    def from_event(self, event: Event) -> Iterable[dict[str, Any]]:
        """
        Given an event, produces one or more JSON objects representing a transaction. The returned object contains
        the given fields, localized in the selected language.

        - `date`
        - `type`
        - `value`
        - `note`
        - `isin`
        - `shares`
        - `fees`
        - `taxes`
        """

        if event.event_type is None:
            return

        if event.event_type == ConditionalEventType.TRADE_INVOICE:
            assert event.value is not None, event
            event.event_type = PPEventType.BUY if event.value < 0 else PPEventType.SELL

        kwargs: _SimpleTransaction = {
            "date": event.date.isoformat() if self.date_with_time else event.date.date().isoformat(),
            "type": self._translate(event.event_type.value) if isinstance(event.event_type, PPEventType) else None,
            "value": self._decimal_format(event.value),
            "note": self._translate(event.note) + " - " + event.title if event.note is not None else event.title,
            "isin": event.isin,
            "shares": self._decimal_format(event.shares, False),
            "fees": self._decimal_format(-event.fees) if event.fees is not None else None,
            "taxes": self._decimal_format(-event.taxes) if event.taxes is not None else None,
        }

        # Special case for saveback events. Example payload: https://github.com/pytr-org/pytr/issues/116#issuecomment-2377491990
        # With saveback, a small amount already invested into a savings plans is invested again, effectively representing
        # a deposit (you get money from Trade Republic) and then a buy of the related asset.
        if event.event_type == ConditionalEventType.SAVEBACK:
            assert event.value is not None, event
            kwargs["type"] = self._translate(PPEventType.BUY.value)
            yield self._localize_keys(kwargs)

            kwargs = kwargs.copy()
            kwargs["type"] = self._translate(PPEventType.DEPOSIT.value)
            kwargs["value"] = self._decimal_format(-event.value)
            kwargs["isin"] = None
            kwargs["shares"] = None
            yield self._localize_keys(kwargs)
        else:
            yield self._localize_keys(kwargs)

    def export(
        self,
        fp: TextIO,
        events: Iterable[Event],
        sort: bool = False,
        format: Literal["json", "csv"] = "csv",
    ) -> None:
        self._log.info("Exporting transactions ...")
        if sort:
            events = sorted(events, key=lambda ev: ev.date)

        transactions = (txn for event in events for txn in self.from_event(event))

        if format == "csv":
            lineterminator = "\n" if platform.system() == "Windows" else "\r\n"
            writer = csv.DictWriter(
                fp, fieldnames=self.fields(), delimiter=self.csv_delimiter, lineterminator=lineterminator
            )
            writer.writeheader()
            writer.writerows(transactions)
        elif format == "json":
            for txn in transactions:
                fp.write(json.dumps(txn))
                fp.write("\n")

        self._log.info("Transactions exported.")

    
    def export_banking4(input_path, output_path, lang='auto'):
        '''
        Create a CSV with most of transactions available for import in banking4
        '''
        log = get_logger(__name__)
        if lang == 'auto':
            locale = getdefaultlocale()[0]
            if locale is None:
                lang = 'en'
            else:
                lang = locale.split('_')[0]
        #Build Strings
        timeline1_loc = os.path.join(input_path,"other_events.json")
        timeline2_loc = os.path.join(input_path,"events_with_documents.json")

        # Read relevant deposit timeline entries
        with open(timeline1_loc, encoding='utf-8') as f:
            timeline1 = json.load(f)
        with open(timeline2_loc, encoding='utf-8') as f:
            timeline2 = json.load(f)    

        # Write deposit_transactions.csv file
        # date, transaction, shares, amount, total, fee, isin, name
        log.info('Write transaction entries')
        with open(output_path, 'w', encoding='utf-8') as f:
            # f.write('Datum;Typ;Stück;amount;Wert;Gebühren;ISIN;name\n')
            csv_fmt = '{date};{type};{value};{label}\n'
            header = csv_fmt.format(date='date', type='type', value='value',label="label")
            f.write(header)

            for event in timeline1+timeline2:
                dateTime = datetime.fromisoformat(event['timestamp'][:19])
                date = dateTime.strftime('%Y-%m-%d')

                try:
                    body = event['body']
                except KeyError:
                    body = ''

                if 'storniert' in body:
                    continue

                # SEPA inflows and outflows 
                if event["eventType"] in ["PAYMENT_INBOUND","INCOMING_TRANSFER","OUTGOING_TRANSFER","OUTGOING_TRANSFER_DELEGATION","INCOMING_TRANSFER_DELEGATION"]:
                    if(event["status"] == "CANCELED"):
                        f.write(csv_fmt.format(date=date, type=clean_strings(event["status"]+" "+event['eventType']+" "+str(event['amount']["value"])), value=0.00,label="Umbuchung"))
                    else:
                        f.write(csv_fmt.format(date=date, type=clean_strings(event['eventType']), value=round(event['amount']["value"],2),label="Umbuchung"))
                # Kauf
                elif event["eventType"] in ["TRADE_INVOICE","ORDER_EXECUTED","TRADE_CORRECTED","trading_trade_executed"]:
                    title = event['title']
                    subtitle = event["subtitle"]
                    if title is None:
                        title = 'no title'
                    if subtitle is None:
                        subtitle = "no subtitle"
                    f.write(csv_fmt.format(date=date, type=clean_strings(title+": "+subtitle), value=round(event['amount']["value"],2),label="Kauf"))
                # Zinsen
                elif event["eventType"] in ["INTEREST_PAYOUT_CREATED","TAX_REFUND","INTEREST_PAYOUT","ssp_tax_correction_invoice"]:
                    title = event['title']
                    subtitle = event["subtitle"]
                    if title is None:
                        title = 'no title'
                    if subtitle is None:
                        subtitle = "no subtitle"
                    f.write(csv_fmt.format(date=date, type=clean_strings(title+": "+subtitle), value=round(event['amount']["value"],2),label="Zinsen"))
                #Debit payments    
                elif event["eventType"] in ["card_successful_transaction","card_successful_atm_withdrawal","card_refund"]:
                    f.write(csv_fmt.format(date=date, type=clean_strings(event["eventType"]+": "+event['title'] ), value=round(event['amount']["value"], 2),label="Ausgabe"))
                #  dividends,
                elif event["eventType"] in ["ssp_corporate_action_invoice_cash"]:
                    #reclassification of
                    if(event["status"] == "CANCELED"):
                        print("WARN: "+str(date)+" DIVIDEND RECLASSIFICATION "+event["subtitle"]+": "+event["title"]+" "+str(event['amount']["value"]))
                        f.write(csv_fmt.format(date=date, type=clean_strings(event["status"]+" "+event["subtitle"]+": "+event["title"]), value=0.00,label="Dividende"))
                    else:
                        f.write(csv_fmt.format(date=date, type=clean_strings(event["subtitle"]+": "+event["title"]), value=round(event['amount']["value"],2),label="Dividende"))
                # legacy dividend payments
                elif event["eventType"] in ["CREDIT"]:
                        f.write(csv_fmt.format(date=date, type=clean_strings(event["subtitle"]+": "+event["title"]), value=round(event['amount']["value"],2),label="Dividende"))
                #Saveback (creates a zero entry just for informational purposes)
                elif event["eventType"] in ["benefits_saveback_execution"]:
                    f.write(csv_fmt.format(date=date, type=clean_strings(event["subtitle"]+": "+event["title"]+": "+str(-1*event["amount"]["value"])), value=0.00, label="Kauf"))
                # Savingsplan
                elif event["eventType"] in ["SAVINGS_PLAN_EXECUTED","SAVINGS_PLAN_INVOICE_CREATED","trading_savingsplan_executed"]:
                    f.write(csv_fmt.format(date=date, type=clean_strings(event["subtitle"]+": "+event["title"]), value=round(event['amount']["value"],2),label="Kauf"))
                #Tax payments
                elif event["eventType"] in ["PRE_DETERMINED_TAX_BASE"]:
                    f.write(csv_fmt.format(date=date, type=clean_strings(event["subtitle"]+": "+event["title"]), value=round(event['amount']["value"],2),label="Steuer"))
                #Card order
                elif event["eventType"] in ["card_order_billed"]:
                    f.write(csv_fmt.format(date=date, type=clean_strings(event["title"]), value=round(event['amount']["value"],2),label="Ausgabe"))
                #Referral
                elif event["eventType"] in ["REFERRAL_FIRST_TRADE_EXECUTED_INVITER"]:
                    f.write(csv_fmt.format(date=date, type=clean_strings(event["title"]+": "+event["subtitle"]),value=round(event['amount']["value"],2),label="Einnahme"))
                #Capital events (e.g. return of capital)
                elif event["eventType"] in ["SHAREBOOKING_TRANSACTIONAL"]:
                    if (event["subtitle"]=="Reinvestierung"):
                        pass
                    else:
                        f.write(csv_fmt.format(date=date, type=clean_strings(event["title"]+": "+event["subtitle"]),value=round(event['amount']["value"],2),label="Kapitalevent"))
                # Events that are not transactions tracked by this function
                elif event["eventType"] in ["current_account_activated","trading_order_expired","EXEMPTION_ORDER_CHANGED","EXEMPTION_ORDER_CHANGE_REQUESTED","AML_SOURCE_OF_WEALTH_RESPONSE_EXECUTED","DEVICE_RESET",
                                            "REFERENCE_ACCOUNT_CHANGED","EXEMPTION_ORDER_CHANGE_REQUESTED_AUTOMATICALLY","CASH_ACCOUNT_CHANGED","ACCOUNT_TRANSFER_INCOMING",
                                            "card_failed_transaction","EMAIL_VALIDATED","PUK_CREATED","SECURITIES_ACCOUNT_CREATED","card_successful_verification",
                                            "ssp_dividend_option_customer_instruction","new_tr_iban","DOCUMENTS_ACCEPTED","EX_POST_COST_REPORT","INSTRUCTION_CORPORATE_ACTION",
                                            "SHAREBOOKING","GESH_CORPORATE_ACTION","GENERAL_MEETING","QUARTERLY_REPORT","DOCUMENTS_ACCEPTED","GENERAL_MEETING","INSTRUCTION_CORPORATE_ACTION",
                                            "DOCUMENTS_CHANGED","MATURITY","YEAR_END_TAX_REPORT","STOCK_PERK_REFUNDED","ORDER_CANCELED","ORDER_EXPIRED","DOCUMENTS_CREATED","CUSTOMER_CREATED","card_failed_verification",
                                            ]:
                    pass
                else:
                    print("ERROR: "+"Type: "+event["eventType"]+"  Title: "+event["title"])

        log.info('transaction creation finished!')

    def clean_strings(text: str):
        return text.replace("\n", "")
