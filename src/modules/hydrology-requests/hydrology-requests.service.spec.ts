import { Test, TestingModule } from '@nestjs/testing';
import { HydrologyRequestsService } from './hydrology-requests.service';

describe('HydrologyRequestsService', () => {
  let service: HydrologyRequestsService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [HydrologyRequestsService],
    }).compile();

    service = module.get<HydrologyRequestsService>(HydrologyRequestsService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });
});
