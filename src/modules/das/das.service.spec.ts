import { Test, TestingModule } from '@nestjs/testing';
import { DasService } from './das.service';

describe('DasService', () => {
  let service: DasService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [DasService],
    }).compile();

    service = module.get<DasService>(DasService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });
});
