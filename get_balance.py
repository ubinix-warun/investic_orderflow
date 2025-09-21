{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 1,
   "id": "d9205306-94da-42fd-8d95-83ad18db3710",
   "metadata": {},
   "outputs": [
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "กรุณา export BINANCE_KEY และ BINANCE_SECRET ก่อนรันสคริปต์\n"
     ]
    },
    {
     "ename": "SystemExit",
     "evalue": "1",
     "output_type": "error",
     "traceback": [
      "An exception has occurred, use %tb to see the full traceback.\n",
      "\u001b[31mSystemExit\u001b[39m\u001b[31m:\u001b[39m 1\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "/opt/anaconda3/envs/grid_trading/lib/python3.11/site-packages/IPython/core/interactiveshell.py:3707: UserWarning: To exit: use 'exit', 'quit', or Ctrl-D.\n",
      "  warn(\"To exit: use 'exit', 'quit', or Ctrl-D.\", stacklevel=1)\n"
     ]
    }
   ],
   "source": [
    "#!/usr/bin/env python3\n",
    "# -*- coding: utf-8 -*-\n",
    "\n",
    "import os, sys\n",
    "import ccxt\n",
    "\n",
    "def main():\n",
    "    key = os.getenv(\"BINANCE_KEY\")\n",
    "    sec = os.getenv(\"BINANCE_SECRET\")\n",
    "    if not key or not sec:\n",
    "        print(\"กรุณา export BINANCE_KEY และ BINANCE_SECRET ก่อนรันสคริปต์\", file=sys.stderr)\n",
    "        sys.exit(1)\n",
    "\n",
    "    ex = ccxt.binance({\n",
    "        \"apiKey\": key,\n",
    "        \"secret\": sec,\n",
    "        \"enableRateLimit\": True,\n",
    "        \"options\": {\"adjustForTimeDifference\": True, \"defaultType\": \"spot\"},\n",
    "    })\n",
    "\n",
    "    try:\n",
    "        ex.load_markets()\n",
    "        bal = ex.fetch_balance()   # SPOT\n",
    "    except Exception as e:\n",
    "        print(f\"ดึงยอดเงินไม่สำเร็จ: {e}\", file=sys.stderr)\n",
    "        sys.exit(1)\n",
    "\n",
    "    free = bal.get(\"free\", {}) or {}\n",
    "    used = bal.get(\"used\", {}) or {}\n",
    "    total = bal.get(\"total\", {}) or {}\n",
    "\n",
    "    rows = []\n",
    "    for asset, tot in total.items():\n",
    "        t = float(tot or 0)\n",
    "        if t > 0:\n",
    "            rows.append((asset, float(free.get(asset, 0) or 0),\n",
    "                               float(used.get(asset, 0) or 0),\n",
    "                               t))\n",
    "    rows.sort(key=lambda r: r[3], reverse=True)\n",
    "\n",
    "    print(\"=== Binance Spot Balances (non-zero) ===\")\n",
    "    print(f\"{'Asset':<8} {'Free':>18} {'Used':>18} {'Total':>18}\")\n",
    "    for a, f, u, t in rows:\n",
    "        print(f\"{a:<8} {f:>18.8f} {u:>18.8f} {t:>18.8f}\")\n",
    "\n",
    "    print(f\"\\nFree USDT: {free.get('USDT', 0)}\")\n",
    "\n",
    "if __name__ == \"__main__\":\n",
    "    main()\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "f36b3cf1-74d1-4c8a-935b-cf732bd6a337",
   "metadata": {},
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python [conda env:grid_trading]",
   "language": "python",
   "name": "conda-env-grid_trading-py"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.11.13"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
