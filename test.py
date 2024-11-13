import requests

url = "https://api.gupshup.io/wa/api/v1/template/msg"

payload = 'channel=whatsapp&source=918951359309&destination=919080991532&src.name=tHmYIgOKgztIGYG000ZhFHQp&template=%7B%22id%22%3A%22edf7cccf-cc07-4afc-a322-667956898dec%22%2C%22params%22%3A%5B%22100%22%2C%22100%22%2C%22100%22%2C%22100%22%2C%22100%22%2C%22100%22%2C%22100%22%2C%22100%22%2C%22100%22%2C%22100%22%2C%22100%22%2C%22100%22%2C%22100%22%2C%22100%22%2C%22100%22%2C%22100%22%2C%22100%22%2C%22100%22%2C%22100%22%2C%22100%22%2C%22100%22%2C%22100%22%2C%22100%22%2C%22100%22%2C%22100%22%2C%22836%22%5D%7D'
headers = {
  'Cache-Control': 'no-cache',
  'Content-Type': 'application/x-www-form-urlencoded',
  'apikey': 'bhn6nqnfpieunjlerhevpfmktlb5gvb2',
  'cache-control': 'no-cache'
}

response = requests.request("POST", url, headers=headers, data=payload)

print(response.text)
