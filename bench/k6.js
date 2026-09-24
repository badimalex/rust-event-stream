import http from 'k6/http';
import { check,fail  } from 'k6';
import { Counter, Rate } from 'k6/metrics';

const successfulInserts = new Counter('successful_inserts');
const requestErrors = new Rate('request_errors');
const status503Counter = new Counter('status_503_overload');  
const status408Counter = new Counter('status_408_timeout'); 

export const options = {
    summaryTrendStats: ['avg', 'p(50)', 'p(95)', 'p(99)', 'max'],
    scenarios: {
        baseline: {
            executor: 'constant-vus',
            vus: parseInt(__ENV.VUS || '10', 10),
            duration: __ENV.DURATION || '10s',
        },
    },
};

const apiKey = __ENV.API_KEY;

if (!apiKey) {
    fail('❌ Ошибка: Переменная окружения API_KEY не задана! Запустите k6 с флагом: -e API_KEY=your_key_here');
}

export default function () {
    const eventId = crypto.randomUUID();

    const params = {
        headers: {
        'Content-Type': 'application/json',
        'X-API-Key': apiKey, 
        },
    };

    
    const body = JSON.stringify({
        event_id: eventId,
        tenant_id: 't-1',
        event_type: 'test',
        timestamp: 123456789,
        payload: 'hello'
    });

     const res = http.post(
        'http://127.0.0.1:3000/v1/events',
        body,
        params
    );

     const is201 =  check(res, {
        'status is 201 (Created)': (r) => r.status === 201,
    });

    if (is201) {
        successfulInserts.add(1);
    } else {
        requestErrors.add(1);
        
        // Фиксируем специфичные коды для доказательства контролируемой перегрузки
        if (res.status === 503) {
            status503Counter.add(1);
        } else if (res.status === 408) {
            status408Counter.add(1);
        }
    }
}