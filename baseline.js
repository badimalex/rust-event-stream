import http from 'k6/http';

export const options = {
    summaryTrendStats: ['avg', 'med', 'p(95)', 'p(99)'],

    scenarios: {
        baseline: {
            executor: 'constant-vus',
            vus: 100,
            duration: '30s',
        },
    },
};

export default function () {
    const body = JSON.stringify({
        event_id: `baseline-${__VU}-${__ITER}`,
        tenant_id: 't-1',
        event_type: 'test',
        timestamp: 123456789,
        payload: 'hello'
    });

    http.post(
        'http://127.0.0.1:3000/v1/events',
        body,
        {
            headers: {
                'Content-Type': 'application/json'
            }
        }
    );
}