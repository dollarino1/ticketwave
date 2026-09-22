#!/usr/bin/env bash
# End-to-end smoke test against a running stack (docker compose --profile apps up).
# It drives the system only through its public door, nginx on :8080, exactly as a
# browser would, and checks the behaviours that matter across service boundaries:
# sign-in, the purchase saga, its compensation, and the live seat stream.
#
#   scripts/smoke.sh                 # against http://localhost:8080
#   BASE_URL=http://host:8080 scripts/smoke.sh
#
# Exit status is 0 only if every check passed.
#
# It uses the gateway's real rate limits (5 registrations and 10 logins a minute per
# address), and deliberately trips the login one at the end. Leave a minute between
# runs, or the next run's sign-ins will be answered with 429.
set -uo pipefail

BASE_URL="${BASE_URL:-http://localhost:8080}"
PASSWORD="correct horse battery"
RUN_ID="$(date +%s)$RANDOM"
WORK="$(mktemp -d)"
trap 'kill $(jobs -p) 2>/dev/null; rm -rf "$WORK"' EXIT

failures=0
pass() { printf '  ok    %s\n' "$1"; }
fail() { printf '  FAIL  %s\n' "$1"; failures=$((failures + 1)); }
check() { # check "description" expected actual
  if [ "$2" = "$3" ]; then pass "$1"; else fail "$1 (expected '$2', got '$3')"; fi
}
section() { printf '\n%s\n' "$1"; }

# field NAME: pull a string field out of JSON on stdin without needing jq.
field() { sed -n "s/.*\"$1\":\"\([^\"]*\)\".*/\1/p" | head -1; }
# status METHOD PATH [curl args...]: HTTP status code only.
status() { local m="$1" p="$2"; shift 2; curl -s -o /dev/null -w '%{http_code}' -X "$m" "$BASE_URL$p" "$@"; }
J='Content-Type: application/json'

section "Waiting for the stack"
for i in $(seq 1 60); do
  [ "$(status GET /api/events)" = "200" ] && break
  [ "$i" = 60 ] && { echo "stack did not become ready at $BASE_URL"; exit 1; }
  sleep 2
done
pass "gateway answers through nginx"

section "Static site"
check "index page served" 200 "$(status GET /)"
check "client-side route falls back to the app" 200 "$(status GET /events/some-id)"
headers="$(curl -sI "$BASE_URL/")"
echo "$headers" | grep -qi '^content-security-policy:' && pass "CSP header present" || fail "CSP header missing"

section "Accounts"
ANN="ann-$RUN_ID@smoke.test"
check "register" 201 "$(status POST /api/auth/register -H "$J" -d "{\"email\":\"$ANN\",\"password\":\"$PASSWORD\"}")"
check "registering the same email again is refused" 409 "$(status POST /api/auth/register -H "$J" -d "{\"email\":\"$ANN\",\"password\":\"$PASSWORD\"}")"
check "a weak password is refused" 400 "$(status POST /api/auth/register -H "$J" -d "{\"email\":\"x-$RUN_ID@smoke.test\",\"password\":\"short\"}")"
check "a wrong password is refused" 401 "$(status POST /api/auth/login -H "$J" -d "{\"email\":\"$ANN\",\"password\":\"wrong wrong wrong\"}")"

curl -s -D "$WORK/login.headers" -c "$WORK/jar" "$BASE_URL/api/auth/login" -H "$J" -d "{\"email\":\"$ANN\",\"password\":\"$PASSWORD\"}" >"$WORK/login.body"
ANN_TOKEN="$(field access_token <"$WORK/login.body")"
[ -n "$ANN_TOKEN" ] && pass "login returns an access token" || fail "login returned no access token"
grep -qi '^set-cookie: refresh_token=.*HttpOnly' "$WORK/login.headers" && pass "refresh token is an HttpOnly cookie" || fail "refresh cookie is not HttpOnly"
grep -qi '^set-cookie: refresh_token=.*SameSite=Strict' "$WORK/login.headers" && pass "refresh cookie is SameSite=Strict" || fail "refresh cookie is not SameSite=Strict"
grep -q 'refresh_token' "$WORK/login.body" && fail "the refresh token leaked into the JSON body" || pass "refresh token is not in the JSON body"

check "refresh without the CSRF header is refused" 403 "$(status POST /api/auth/refresh -b "$WORK/jar")"
check "refresh with the CSRF header works" 200 "$(status POST /api/auth/refresh -b "$WORK/jar" -c "$WORK/jar" -H 'X-Requested-With: ticketwave-web')"

section "Authorization"
check "orders need a token" 401 "$(status GET /api/orders/00000000-0000-4000-8000-000000000000)"
check "an ordinary user cannot create events" 403 "$(status POST /api/events -H "$J" -H "Authorization: Bearer $ANN_TOKEN" -d '{"name":"x","seat_count":1}')"
check "an ordinary user cannot read analytics" 403 "$(status GET /api/analytics/events -H "Authorization: Bearer $ANN_TOKEN")"

# The organizer address is configured in compose (ORGANIZER_EMAILS). Registering it
# may answer 409 on a reused database, which is fine.
ORG="organizer@ticketwave.test"
status POST /api/auth/register -H "$J" -d "{\"email\":\"$ORG\",\"password\":\"$PASSWORD\"}" >/dev/null
ORG_TOKEN="$(curl -s "$BASE_URL/api/auth/login" -H "$J" -d "{\"email\":\"$ORG\",\"password\":\"$PASSWORD\"}" | field access_token)"
[ -n "$ORG_TOKEN" ] && pass "the organizer can sign in" || fail "the organizer could not sign in"

section "Events and the seat map"
EVENT_ID="$(curl -s "$BASE_URL/api/events" -H "$J" -H "Authorization: Bearer $ORG_TOKEN" -d "{\"name\":\"Smoke $RUN_ID\",\"seat_count\":8}" | field event_id)"
[ -n "$EVENT_ID" ] && pass "organizer creates an event" || { fail "event was not created"; exit 1; }
SEATS=($(curl -s "$BASE_URL/api/events/$EVENT_ID/seats" | grep -o '"id":"[^"]*"' | sed 's/"id":"\(.*\)"/\1/'))
check "the event has 8 seats" 8 "${#SEATS[@]}"

section "Live seat stream"
curl -sN --max-time 30 "$BASE_URL/api/events/$EVENT_ID/stream" >"$WORK/stream" 2>/dev/null &
sleep 2

section "Purchase"
order="$(curl -s "$BASE_URL/api/orders" -H "$J" -H "Authorization: Bearer $ANN_TOKEN" -d "{\"event_id\":\"$EVENT_ID\",\"seat_ids\":[\"${SEATS[0]}\"]}")"
ORDER_ID="$(echo "$order" | field order_id)"
check "buying one seat confirms the order" confirmed "$(echo "$order" | field status)"
mine="$(curl -s "$BASE_URL/api/orders/$ORDER_ID" -H "Authorization: Bearer $ANN_TOKEN")"
check "the buyer is charged 50.00 (5000 cents)" 5000 "$(echo "$mine" | sed -n 's/.*"amount_cents":\([0-9]*\).*/\1/p')"

BOB="bob-$RUN_ID@smoke.test"
status POST /api/auth/register -H "$J" -d "{\"email\":\"$BOB\",\"password\":\"$PASSWORD\"}" >/dev/null
BOB_TOKEN="$(curl -s "$BASE_URL/api/auth/login" -H "$J" -d "{\"email\":\"$BOB\",\"password\":\"$PASSWORD\"}" | field access_token)"
check "someone else's order looks like it does not exist" 404 "$(status GET "/api/orders/$ORDER_ID" -H "Authorization: Bearer $BOB_TOKEN")"
check "the same seat cannot be sold twice" 409 "$(status POST /api/orders -H "$J" -H "Authorization: Bearer $BOB_TOKEN" -d "{\"event_id\":\"$EVENT_ID\",\"seat_ids\":[\"${SEATS[0]}\"]}")"
check "a client cannot name its own price" 400 "$(status POST /api/orders -H "$J" -H "Authorization: Bearer $BOB_TOKEN" -d "{\"event_id\":\"$EVENT_ID\",\"seat_ids\":[\"${SEATS[1]}\"],\"amount_cents\":1}")"

section "Payment failure compensates"
# 7 seats cost 35000 cents, which the fake payment service declines (divisible by 7).
seven="\"${SEATS[1]}\",\"${SEATS[2]}\",\"${SEATS[3]}\",\"${SEATS[4]}\",\"${SEATS[5]}\",\"${SEATS[6]}\",\"${SEATS[7]}\""
check "a declined card is reported as 402" 402 "$(status POST /api/orders -H "$J" -H "Authorization: Bearer $BOB_TOKEN" -d "{\"event_id\":\"$EVENT_ID\",\"seat_ids\":[$seven]}")"
available="$(curl -s "$BASE_URL/api/events/$EVENT_ID/seats" | grep -o '"status":"available"' | wc -l | tr -d ' ')"
check "the seats held for the failed order were released" 7 "$available"

section "Events reached the live stream"
for _ in $(seq 1 20); do
  grep -q '"state":"available"' "$WORK/stream" && break
  sleep 0.5
done
grep -q '"state":"held"' "$WORK/stream" && pass "stream announced seats as held" || fail "no 'held' update on the stream"
grep -q '"state":"sold"' "$WORK/stream" && pass "stream announced the sale" || fail "no 'sold' update on the stream"
grep -q '"state":"available"' "$WORK/stream" && pass "stream announced the released seats" || fail "no 'available' update on the stream"

section "Analytics"
# The projection is asynchronous (Kafka), so allow it a moment.
sold=""
for _ in $(seq 1 20); do
  sold="$(curl -s "$BASE_URL/api/analytics/events" -H "Authorization: Bearer $ORG_TOKEN" | grep -o "\"event_id\":\"$EVENT_ID\"[^}]*" | sed -n 's/.*"tickets_sold":\([0-9]*\).*/\1/p')"
  [ "$sold" = "1" ] && break
  sleep 0.5
done
check "the organizer's dashboard counts the sale" 1 "$sold"

section "Rate limiting"
codes=""
for _ in $(seq 1 12); do codes="$codes $(status POST /api/auth/login -H "$J" -d "{\"email\":\"nobody-$RUN_ID@smoke.test\",\"password\":\"wrong wrong wrong\"}")"; done
echo "$codes" | grep -q 429 && pass "repeated login attempts get 429" || fail "no 429 after 12 rapid logins:$codes"

printf '\n'
if [ "$failures" -eq 0 ]; then echo "SMOKE TEST PASSED"; else echo "SMOKE TEST FAILED: $failures check(s)"; exit 1; fi
